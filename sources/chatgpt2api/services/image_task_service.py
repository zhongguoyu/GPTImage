from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from services.config import DATA_DIR, config
from services.content_filter import request_text
from services.log_service import LOG_TYPE_CALL, is_safe_to_log_url, log_service
from services.openai_backend_api import ImageRateLimitedError
from services.protocol import openai_v1_image_edit, openai_v1_image_generations
from services.quota_service import QuotaSettlementError, quota_service
from services.scheduler.context import current_execution_context
from services.scheduler.errors import SchedulerError
from services.scheduler.models import Lane
from services.scheduler.service import scheduler_service
from services.user_key_concurrency import (
    UserKeyConcurrencyLease,
    UserKeyConcurrencyLimitError,
    UserKeyConcurrencyLimiter,
    user_key_concurrency,
)
from utils.log import logger
from utils.helper import anonymize_token, is_codex_image_model, split_image_model

TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_ERROR = "error"
TERMINAL_STATUSES = {TASK_STATUS_SUCCESS, TASK_STATUS_ERROR}
UNFINISHED_STATUSES = {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}
QUOTA_RESERVATION_GRACE_SECONDS = 5 * 60
IMAGE_TASK_MAX_ROWS = 10000
SPOOLED_INPUT_PATH_KEY = "_image_task_spool_path"


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _timestamp(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return 0.0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:26], fmt).timestamp()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _clean(value: object, default: str = "") -> str:
    return str(value or default).strip()


def _owner_id(identity: dict[str, object]) -> str:
    return _clean(identity.get("id")) or "anonymous"


def _task_key(owner_id: str, task_id: str) -> str:
    return f"{owner_id}:{task_id}"


def _collect_image_urls(data: list[Any]) -> list[str]:
    urls: list[str] = []
    for item in data:
        if isinstance(item, dict):
            url = item.get("url")
            if isinstance(url, str) and is_safe_to_log_url(url):
                urls.append(url)
    return urls


def _task_result_metadata(task: dict[str, Any]) -> dict[str, Any] | None:
    """Persist only recoverable image URLs, never inline image bytes."""
    data = task.get("data")
    if not isinstance(data, list):
        return None
    urls = _collect_image_urls(data)
    return {
        "urls": urls,
        "count": len(data),
        "available": bool(urls),
    }


def _requested_image_units(payload: dict[str, Any]) -> int:
    try:
        return max(1, int(payload.get("n") or 1))
    except (TypeError, ValueError, OverflowError):
        return 1


def _is_resumable_timeout(error: BaseException, conversation_id: str) -> bool:
    if not conversation_id:
        return False
    code = _clean(getattr(error, "code", "")).lower()
    message = str(error).lower()
    return code == "image_poll_timeout" or "timeout" in message or "超时" in message


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    item = {
        "id": task.get("id"),
        "status": task.get("status"),
        "mode": task.get("mode"),
        "model": task.get("model"),
        "size": task.get("size"),
        "quality": task.get("quality"),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
    }
    if task.get("conversation_id"):
        item["conversation_id"] = task.get("conversation_id")
    if task.get("data") is not None:
        item["data"] = task.get("data")
    if task.get("usage") is not None:
        item["usage"] = task.get("usage")
    if task.get("error"):
        item["error"] = task.get("error")
    if task.get("progress"):
        item["progress"] = task.get("progress")
    if task.get("duration_ms") is not None:
        item["duration_ms"] = task.get("duration_ms")
    if task.get("status") in (TASK_STATUS_RUNNING, TASK_STATUS_QUEUED):
        if task.get("status") == TASK_STATUS_RUNNING:
            # RUNNING 状态仅在 started_ts 被设置后（image_stream_resolve_start）才计时
            base_ts = task.get("started_ts")
        else:
            # QUEUED 状态从 created_ts 开始计时（排队等待中）
            base_ts = task.get("created_ts") or task.get("updated_ts")
        if base_ts:
            item["elapsed_secs"] = round(time.time() - base_ts, 1)
    return item


class ImageTaskService:
    def __init__(
        self,
        path: Path,
        *,
        generation_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_generations.handle,
        edit_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_edit.handle,
        retention_days_getter: Callable[[], int] | None = None,
        scheduler_service_instance: Any | None = None,
        state_store: Any | None = None,
        task_writer: Any | None = None,
        quota_service_instance: Any | None = None,
        concurrency_limiter: UserKeyConcurrencyLimiter | None = None,
        spool_dir: Path | None = None,
    ):
        self.path = path
        self.generation_handler = generation_handler
        self.edit_handler = edit_handler
        self.retention_days_getter = retention_days_getter or (lambda: config.image_retention_days)
        self.scheduler_service = scheduler_service_instance
        self.state_store = state_store
        self.task_writer = task_writer or getattr(scheduler_service_instance, "task_writer", None)
        self.quota_service = quota_service_instance or quota_service
        self.concurrency_limiter = concurrency_limiter or user_key_concurrency
        self.spool_dir = spool_dir or path.parent / ".image_task_spool"
        self._fallback_executor = ThreadPoolExecutor(
            max_workers=max(1, int(config.get_scheduler_settings().lanes[Lane.IMAGE_GENERATION].worker_count)),
            thread_name_prefix="image-task-fallback",
        )
        self._lock = threading.RLock()
        self._tasks: dict[str, dict[str, Any]] = {}
        self._admission_tasks: dict[str, asyncio.Task[Any]] = {}
        self._concurrency_leases: dict[str, UserKeyConcurrencyLease] = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cleanup_spool_dir()
        with self._lock:
            self._tasks = self._load_state_store_locked() if self.state_store is not None else self._load_locked()
            changed = self._recover_unfinished_locked()
            changed = self._cleanup_locked() or changed
            if changed:
                self._save_locked()

    def _load_state_store_locked(self) -> dict[str, dict[str, Any]]:
        try:
            self.state_store.initialize()
            has_migration_marker = getattr(self.state_store, "has_migration_marker", None)
            if self.path.exists() and (not callable(has_migration_marker) or not has_migration_marker()):
                self.state_store.import_legacy_tasks(self.path)
            recoverable = {
                str(item.get("task_key")): item
                for item in self.state_store.load_recoverable_tasks(
                    task_types=("generate", "edit"),
                )
            }
            tasks: dict[str, dict[str, Any]] = {}
            for record in recoverable.values():
                converted = self._task_from_state_record(record, recoverable=True)
                if converted is not None:
                    task_key, task = converted
                    tasks[task_key] = task
            return tasks
        except Exception:
            return self._load_locked()

    def _task_from_state_record(
        self,
        record: dict[str, object],
        *,
        recoverable: bool = False,
    ) -> tuple[str, dict[str, Any]] | None:
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        task_id = _clean(record.get("task_id"))
        owner = _clean(record.get("owner_id"))
        if not task_id or not owner:
            return None
        task_key = _task_key(owner, task_id)
        status = _clean(record.get("status"), TASK_STATUS_ERROR)
        remote_task_id = _clean(record.get("remote_task_id"))
        persisted_result = record.get("result")
        persisted_urls = persisted_result.get("urls") if isinstance(persisted_result, dict) else None
        if not isinstance(persisted_urls, list) and isinstance(persisted_result, dict):
            persisted_urls = persisted_result.get("data")
        restored_urls = [
            str(url)
            for url in (persisted_urls if isinstance(persisted_urls, list) else [])
            if isinstance(url, str) and is_safe_to_log_url(url)
        ]
        result_available = bool(
            isinstance(persisted_result, dict)
            and persisted_result.get("available")
            and restored_urls
        )
        resume_on_start = bool(recoverable and remote_task_id and status in UNFINISHED_STATUSES)
        if status == "unknown":
            status = TASK_STATUS_ERROR
        if status in UNFINISHED_STATUSES and not remote_task_id:
            status = TASK_STATUS_ERROR
        if status == TASK_STATUS_SUCCESS and not result_available:
            status = TASK_STATUS_ERROR
        task = {
            "id": task_id,
            "owner_id": owner,
            "status": status,
            "mode": "edit" if record.get("mode") == "edit" else "generate",
            "model": _clean(record.get("model"), "gpt-image-2"),
            "size": _clean(metadata.get("size")),
            "quality": _clean(metadata.get("quality"), "auto"),
            "created_at": _clean(metadata.get("created_at"), _now_iso()),
            "updated_at": _now_iso(),
            "created_ts": record.get("created_at"),
            "updated_ts": record.get("updated_at"),
        }
        quota_reservation_id = _clean(metadata.get("quota_reservation_id"))
        if quota_reservation_id:
            task["quota_reservation_id"] = quota_reservation_id
            try:
                task["quota_requested_units"] = max(1, int(metadata.get("quota_requested_units") or 1))
            except (TypeError, ValueError, OverflowError):
                task["quota_requested_units"] = 1
            quota_status = _clean(metadata.get("quota_status"))
            task["quota_status"] = quota_status or ("committed" if status == TASK_STATUS_SUCCESS else "reserved")
            try:
                task["quota_committed_units"] = max(0, int(metadata.get("quota_committed_units") or 0))
            except (TypeError, ValueError, OverflowError):
                task["quota_committed_units"] = 0
        account_ref = _clean(metadata.get("account_ref"))
        if account_ref:
            task["account_ref"] = account_ref
        if remote_task_id:
            task["conversation_id"] = remote_task_id
        if result_available:
            task["data"] = [{"url": url} for url in restored_urls]
        if record.get("error"):
            task["error"] = _clean(record.get("error"))
        if status == TASK_STATUS_ERROR and not task.get("error"):
            task["error"] = (
                "服务已重启，未完成的图片任务提交结果未知"
                if not remote_task_id
                else "服务重启后任务结果不可恢复"
            )
        if resume_on_start:
            task["_resume_on_start"] = True
        return task_key, task

    def submit_generation(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        quality: str = "auto",
        base_url: str = "",
    ) -> dict[str, Any]:
        payload = {
            "prompt": prompt,
            "model": model,
            "n": 1,
            "size": size,
            "quality": quality,
            "response_format": "url",
            "base_url": base_url,
        }
        return self._submit(identity, client_task_id=client_task_id, mode="generate", payload=payload)

    async def start(self) -> None:
        """Resume persisted image polls after the scheduler event loop starts."""
        pending: list[tuple[str, str, str, str, str]] = []
        with self._lock:
            for key, task in self._tasks.items():
                if not task.pop("_resume_on_start", False):
                    continue
                conversation_id = _clean(task.get("conversation_id"))
                if not conversation_id:
                    continue
                if not self._renew_task_quota(task):
                    reservation_status = self._reservation_status(_clean(task.get("quota_reservation_id")))
                    task["status"] = TASK_STATUS_ERROR
                    task["error"] = "图片任务额度预留已失效，无法恢复"
                    if reservation_status in {"committed", "released", "expired"}:
                        task["quota_status"] = reservation_status
                    task["updated_at"] = _now_iso()
                    task["updated_ts"] = time.time()
                    self._save_task_locked(task, synchronous=False)
                    continue
                try:
                    self._acquire_task_concurrency(
                        key,
                        {"id": _clean(task.get("owner_id")), "role": "user"},
                        existing=True,
                    )
                    task["status"] = TASK_STATUS_RUNNING
                    task["error"] = ""
                    task["updated_at"] = _now_iso()
                    task["updated_ts"] = time.time()
                    self._save_task_locked(task, synchronous=False)
                except BaseException:
                    self._release_task_concurrency(key)
                    raise
                pending.append(
                    (
                        key,
                        conversation_id,
                        _clean(task.get("owner_id")),
                        _clean(task.get("mode"), "generate"),
                        _clean(task.get("account_ref")),
                    )
                )
        for key, conversation_id, owner, mode, account_ref in pending:
            with self._lock:
                task = self._tasks.get(key) or {}
                model = _clean(task.get("model"), "gpt-image-2")
            recovery_identity = {"id": owner, "name": "scheduler-recovery", "role": "user"}
            try:
                self._dispatch_resume_poll(
                    key,
                    conversation_id,
                    config.image_poll_timeout_secs,
                    recovery_identity,
                    mode,
                    model,
                    account_ref,
                )
            except BaseException as exc:
                self._release_task_concurrency(key)
                self._release_task_quota(key)
                self._update_task(key, status=TASK_STATUS_ERROR, error=str(exc) or "scheduler recovery failed")
                raise

    def shutdown(self, *, wait: bool = False) -> None:
        self._fallback_executor.shutdown(wait=wait, cancel_futures=not wait)

    async def submit_generation_async(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        quality: str = "auto",
        base_url: str = "",
    ) -> dict[str, Any]:
        payload = {
            "prompt": prompt,
            "model": model,
            "n": 1,
            "size": size,
            "quality": quality,
            "response_format": "url",
            "base_url": base_url,
        }
        return await self._submit_async(identity, client_task_id=client_task_id, mode="generate", payload=payload)

    def submit_edit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        quality: str = "auto",
        base_url: str = "",
        images: list[tuple[bytes, str, str]] | None = None,
        masks: list[tuple[bytes, str, str]] | None = None,
        comments: list[dict[str, Any]] | None = None,
        markup_overlays: list[tuple[bytes, str, str]] | None = None,
        markup_strokes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload = self._build_edit_payload(
            prompt=prompt,
            model=model,
            size=size,
            quality=quality,
            base_url=base_url,
            images=images,
            masks=masks,
            comments=comments,
            markup_overlays=markup_overlays,
            markup_strokes=markup_strokes,
        )
        return self._submit(identity, client_task_id=client_task_id, mode="edit", payload=payload)

    async def submit_edit_async(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        quality: str = "auto",
        base_url: str = "",
        images: list[tuple[bytes, str, str]] | None = None,
        masks: list[tuple[bytes, str, str]] | None = None,
        comments: list[dict[str, Any]] | None = None,
        markup_overlays: list[tuple[bytes, str, str]] | None = None,
        markup_strokes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload = self._build_edit_payload(
            prompt=prompt,
            model=model,
            size=size,
            quality=quality,
            base_url=base_url,
            images=images,
            masks=masks,
            comments=comments,
            markup_overlays=markup_overlays,
            markup_strokes=markup_strokes,
        )
        del images, masks, markup_overlays
        return await self._submit_async(identity, client_task_id=client_task_id, mode="edit", payload=payload)

    def _build_edit_payload(
        self,
        *,
        prompt: str,
        model: str,
        size: str | None,
        quality: str,
        base_url: str,
        images: list[tuple[bytes, str, str]] | None,
        masks: list[tuple[bytes, str, str]] | None,
        comments: list[dict[str, Any]] | None = None,
        markup_overlays: list[tuple[bytes, str, str]] | None = None,
        markup_strokes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        spooled_images: list[dict[str, str]] = []
        spooled_masks: list[dict[str, str]] = []
        spooled_markups: list[dict[str, str]] = []
        try:
            spooled_images = self._spool_inputs(images or [])
            spooled_masks = self._spool_inputs(masks or [])
            spooled_markups = self._spool_inputs(markup_overlays or [])
        except BaseException:
            self._cleanup_spool_descriptors(spooled_images + spooled_masks + spooled_markups)
            raise
        payload: dict[str, Any] = {
            "prompt": prompt,
            "images": spooled_images,
            "mask": spooled_masks,
            "markup": spooled_markups,
            "model": model,
            "n": 1,
            "size": size,
            "quality": quality,
            "response_format": "url",
            "base_url": base_url,
        }
        if comments:
            payload["comments"] = comments
        if markup_strokes:
            payload["markup_strokes"] = markup_strokes
        return payload

    def _spool_inputs(self, inputs: list[tuple[bytes, str, str]]) -> list[dict[str, str]]:
        if not inputs:
            return []
        path: str | None = None
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        descriptors: list[dict[str, str]] = []
        try:
            for data, filename, mime_type in inputs:
                current_path: str | None = None
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="wb",
                        dir=self.spool_dir,
                        prefix="image-task-",
                        suffix=".bin",
                        delete=False,
                    ) as handle:
                        current_path = handle.name
                        handle.write(data)
                except BaseException:
                    if current_path:
                        self._cleanup_spool_descriptors([{SPOOLED_INPUT_PATH_KEY: current_path}])
                    raise
                path = current_path
                assert path is not None
                descriptors.append(
                    {
                        SPOOLED_INPUT_PATH_KEY: path,
                        "filename": str(filename or "image"),
                        "mime_type": str(mime_type or "application/octet-stream"),
                    }
                )
        except BaseException:
            if path:
                self._cleanup_spool_descriptors([{SPOOLED_INPUT_PATH_KEY: path}])
            self._cleanup_spool_descriptors(descriptors)
            raise
        return descriptors

    @staticmethod
    def _spool_paths(value: object) -> list[Path]:
        if not isinstance(value, list):
            return []
        paths: list[Path] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            path = item.get(SPOOLED_INPUT_PATH_KEY)
            if isinstance(path, str) and path:
                paths.append(Path(path))
        return paths

    @classmethod
    def _cleanup_spool_descriptors(cls, descriptors: object) -> None:
        if not isinstance(descriptors, list):
            return
        for path in cls._spool_paths(descriptors):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

    def _cleanup_payload_spool(self, payload: dict[str, Any]) -> None:
        self._cleanup_spool_descriptors(payload.get("images"))
        self._cleanup_spool_descriptors(payload.get("mask"))
        self._cleanup_spool_descriptors(payload.get("markup"))

    def _cleanup_spool_dir(self) -> None:
        try:
            self.spool_dir.mkdir(parents=True, exist_ok=True)
            for path in self.spool_dir.glob("image-task-*.bin"):
                path.unlink(missing_ok=True)
        except OSError:
            logger.warning(f"image task spool cleanup failed: {self.spool_dir}")

    def _materialize_spooled_inputs(
        self,
        value: object,
    ) -> list[tuple[bytes, str, str]]:
        if not isinstance(value, list):
            return []
        materialized: list[tuple[bytes, str, str]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            path = item.get(SPOOLED_INPUT_PATH_KEY)
            if not isinstance(path, str) or not path:
                continue
            data = Path(path).read_bytes()
            materialized.append(
                (
                    data,
                    str(item.get("filename") or "image"),
                    str(item.get("mime_type") or "application/octet-stream"),
                )
            )
        return materialized

    def _materialize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if (
            not self._spool_paths(payload.get("images"))
            and not self._spool_paths(payload.get("mask"))
            and not self._spool_paths(payload.get("markup"))
        ):
            return payload
        materialized = dict(payload)
        materialized["images"] = self._materialize_spooled_inputs(payload.get("images"))
        materialized["mask"] = self._materialize_spooled_inputs(payload.get("mask"))
        materialized["markup"] = self._materialize_spooled_inputs(payload.get("markup"))
        return materialized

    def list_tasks(self, identity: dict[str, object], task_ids: list[str]) -> dict[str, Any]:
        owner = _owner_id(identity)
        requested_ids = [_clean(task_id) for task_id in task_ids if _clean(task_id)]
        with self._lock:
            if self._cleanup_locked():
                self._save_locked()
            items = []
            missing_ids = []
            for task_id in requested_ids:
                task = self._tasks.get(_task_key(owner, task_id))
                if task is None and self.state_store is not None:
                    record = self.state_store.get_task(_task_key(owner, task_id))
                    converted = self._task_from_state_record(record) if record is not None else None
                    task = converted[1] if converted is not None else None
                if task is None:
                    missing_ids.append(task_id)
                else:
                    items.append(_public_task(task))
            if not requested_ids:
                task_by_key = {
                    key: task
                    for key, task in self._tasks.items()
                    if task.get("owner_id") == owner
                }
                if self.state_store is not None:
                    try:
                        max_rows = max(1, int(config.image_task_max_rows))
                    except (TypeError, ValueError):
                        max_rows = IMAGE_TASK_MAX_ROWS
                    for record in self.state_store.list_tasks(
                        owner_id=owner,
                        limit=max_rows,
                        task_types=("generate", "edit"),
                    ):
                        converted = self._task_from_state_record(record)
                        if converted is not None:
                            task_by_key.setdefault(converted[0], converted[1])
                items = [_public_task(task) for task in task_by_key.values()]
                items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
                missing_ids = []
            return {"items": items, "missing_ids": missing_ids}

    def _quota_endpoint(self, mode: str) -> str:
        return "/api/image-tasks/edits" if mode == "edit" else "/api/image-tasks/generations"

    def _quota_expiry(self) -> float:
        return time.time() + max(1, int(config.image_poll_timeout_secs)) + QUOTA_RESERVATION_GRACE_SECONDS

    def _reserve_task_quota(
        self,
        task: dict[str, Any],
        identity: dict[str, object],
        key: str,
        mode: str,
        payload: dict[str, Any],
    ) -> None:
        requested_units = _requested_image_units(payload)
        reservation = self.quota_service.reserve(
            identity,
            "image",
            requested_units,
            request_id=key,
            endpoint=self._quota_endpoint(mode),
            expires_at=self._quota_expiry(),
        )
        task.update(
            {
                "quota_reservation_id": reservation.reservation_id,
                "quota_requested_units": requested_units,
                "quota_status": "reserved",
                "quota_committed_units": 0,
            }
        )

    def _reservation_status(self, reservation_id: str) -> str:
        getter = getattr(self.quota_service, "get_reservation", None)
        if not callable(getter):
            return ""
        try:
            reservation = getter(reservation_id)
        except Exception:
            return ""
        return _clean(reservation.get("status")) if isinstance(reservation, dict) else ""

    def _persist_quota_state_locked(self, task: dict[str, Any]) -> None:
        task["updated_at"] = _now_iso()
        task["updated_ts"] = time.time()
        self._save_task_locked(task, synchronous=False)

    def _mark_quota_state_locked(
        self,
        task: dict[str, Any],
        status: str,
        committed_units: int | None = None,
    ) -> None:
        task["quota_status"] = status
        if committed_units is not None:
            task["quota_committed_units"] = max(0, int(committed_units))
        self._persist_quota_state_locked(task)

    def _keep_quota_for_resume(self, task: dict[str, Any]) -> bool:
        return (
            task.get("status") == TASK_STATUS_ERROR
            and bool(_clean(task.get("conversation_id")))
            and _is_resumable_timeout(RuntimeError(_clean(task.get("error"))), _clean(task.get("conversation_id")))
        )

    def _release_task_quota(self, key: str) -> bool:
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                return False
            reservation_id = _clean(task.get("quota_reservation_id"))
            if not reservation_id or _clean(task.get("quota_status")) in {"committed", "released", "expired"}:
                return True
            if self._keep_quota_for_resume(task):
                return False
            try:
                released = bool(self.quota_service.release(reservation_id))
            except Exception as exc:
                logger.error({"event": "image_task_quota_release_failed", "task_key": key, "error": str(exc)})
                return False
            status = self._reservation_status(reservation_id)
            if status in {"committed", "released", "expired"}:
                self._mark_quota_state_locked(task, status)
            else:
                self._mark_quota_state_locked(task, "released" if released or not status else status)
            return True

    def _commit_task_quota(self, key: str, data: list[Any]) -> bool:
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                return False
            reservation_id = _clean(task.get("quota_reservation_id"))
            if not reservation_id:
                return True
            quota_status = _clean(task.get("quota_status"))
            if quota_status == "committed":
                return True
            if quota_status in {"released", "expired"}:
                return False
            committed_units = len(data)
            try:
                committed = bool(self.quota_service.commit(reservation_id, committed_units))
            except Exception:
                self._release_task_quota(key)
                raise
            if committed:
                self._mark_quota_state_locked(task, "committed", committed_units)
                return True
            status = self._reservation_status(reservation_id)
            if status == "committed":
                self._mark_quota_state_locked(task, status, committed_units)
                return True
            if status in {"released", "expired"}:
                self._mark_quota_state_locked(task, status)
            else:
                self._release_task_quota(key)
            return False

    def _renew_task_quota(self, task: dict[str, Any]) -> bool:
        reservation_id = _clean(task.get("quota_reservation_id"))
        if not reservation_id or _clean(task.get("quota_status")) != "reserved":
            return not reservation_id or _clean(task.get("quota_status")) == "committed"
        renew = getattr(self.quota_service, "renew", None)
        if not callable(renew):
            return True
        try:
            return bool(renew(reservation_id, expires_at=self._quota_expiry()))
        except Exception as exc:
            logger.error({"event": "image_task_quota_renew_failed", "error": str(exc)})
            return False

    def _acquire_task_concurrency(
        self,
        key: str,
        identity: dict[str, object],
        *,
        existing: bool = False,
    ) -> None:
        lease = (
            self.concurrency_limiter.acquire_existing(identity, "image")
            if existing
            else self.concurrency_limiter.try_acquire(identity, "image")
        )
        if lease is None:
            raise UserKeyConcurrencyLimitError("image")
        with self._lock:
            self._concurrency_leases[key] = lease

    def _release_task_concurrency(self, key: str) -> None:
        with self._lock:
            lease = self._concurrency_leases.pop(key, None)
        if lease is not None:
            lease.release()

    async def _submit_async(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        mode: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        task_id = _clean(client_task_id)
        if not task_id:
            self._cleanup_payload_spool(payload)
            raise ValueError("client_task_id is required")
        owner = _owner_id(identity)
        key = _task_key(owner, task_id)
        result = self._submit(identity, client_task_id=task_id, mode=mode, payload=payload)
        admission = self._admission_tasks.get(key)
        if admission is None:
            return result
        try:
            await admission
        except BaseException:
            with self._lock:
                self._release_task_concurrency(key)
                self._release_task_quota(key)
                self._tasks.pop(key, None)
                if self.state_store is not None:
                    if self.task_writer is not None:
                        self.task_writer.delete(key)
                    else:
                        self.state_store.delete_task(key)
                else:
                    self._save_locked()
            raise
        return result

    def _submit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        mode: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        task_id = _clean(client_task_id)
        if not task_id:
            self._cleanup_payload_spool(payload)
            raise ValueError("client_task_id is required")
        owner = _owner_id(identity)
        key = _task_key(owner, task_id)
        now = _now_iso()
        should_start = False
        with self._lock:
            cleaned = self._cleanup_locked()
            task = self._tasks.get(key)
            if task is None and self.state_store is not None:
                record = self.state_store.get_task(key)
                converted = self._task_from_state_record(record) if record is not None else None
                if converted is not None:
                    task = converted[1]
                    if task.get("status") in UNFINISHED_STATUSES:
                        self._tasks[key] = task
            if task is not None:
                self._cleanup_payload_spool(payload)
                if cleaned:
                    self._save_locked()
                return _public_task(task)
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": TASK_STATUS_QUEUED,
                "mode": mode,
                "model": _clean(payload.get("model"), "gpt-image-2"),
                "size": _clean(payload.get("size")),
                "quality": _clean(payload.get("quality"), "auto"),
                "created_at": now,
                "updated_at": now,
                "created_ts": time.time(),
            }
            try:
                self._acquire_task_concurrency(key, identity)
                self._reserve_task_quota(task, identity, key, mode, payload)
            except BaseException:
                self._release_task_concurrency(key)
                self._cleanup_payload_spool(payload)
                raise
            self._tasks[key] = task
            try:
                self._save_task_locked(task, synchronous=False)
            except BaseException:
                self._release_task_concurrency(key)
                self._cleanup_payload_spool(payload)
                self._tasks.pop(key, None)
                reservation_id = _clean(task.get("quota_reservation_id"))
                if reservation_id:
                    try:
                        self.quota_service.release(reservation_id)
                    except Exception as exc:
                        logger.error({"event": "image_task_quota_release_failed", "task_key": key, "error": str(exc)})
                raise
            should_start = True

        if should_start:
            try:
                self._dispatch_task(
                    key,
                    mode,
                    payload,
                    dict(identity),
                    _clean(payload.get("model"), "gpt-image-2"),
                )
            except BaseException:
                self._cleanup_payload_spool(payload)
                self._release_task_concurrency(key)
                self._release_task_quota(key)
                with self._lock:
                    self._tasks.pop(key, None)
                    if self.state_store is not None:
                        if self.task_writer is not None:
                            self.task_writer.delete(key)
                        else:
                            self.state_store.delete_task(key)
                    else:
                        self._save_locked()
                raise
        return _public_task(task)

    def _dispatch_task(
        self,
        key: str,
        mode: str,
        payload: dict[str, Any],
        identity: dict[str, object],
        model: str,
    ) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._fallback_executor.submit(
                self._run_task,
                key,
                mode,
                payload,
                identity,
                model,
            )
        else:
            admission = asyncio.create_task(
                self._admit_task(
                    key,
                    mode,
                    payload,
                    identity,
                    model,
                )
            )
            self._admission_tasks[key] = admission
            self._watch_admission_task(key, admission, payload)

    async def _admit_task(
        self,
        key: str,
        mode: str,
        payload: dict[str, Any],
        identity: dict[str, object],
        model: str,
    ) -> None:
        from services import sub2_gateway
        from services.protocol.image_upstream import configured
        if configured() or sub2_gateway.enabled() or self.scheduler_service is None or not getattr(
            getattr(self.scheduler_service, "settings", None), "enabled", True
        ):
            self._fallback_executor.submit(self._run_task, key, mode, payload, identity, model)
            return
        requirement = (
            self.scheduler_service.classify_image_edit(payload)
            if mode == "edit"
            else self.scheduler_service.classify_image_generation(payload)
        )
        future = await self.scheduler_service.admit_background(
            requirement,
            lambda _lease, _cancel: self._run_task(
                key,
                mode,
                payload,
                identity,
                model,
                raise_on_error=True,
            ),
        )
        self._watch_background_future(key, future, payload)

    def _watch_background_future(
        self,
        key: str,
        future: Any,
        payload: dict[str, Any] | None = None,
    ) -> None:
        def completed(done: Any) -> None:
            self._cleanup_payload_spool(payload or {})
            if done.cancelled():
                error = "scheduler task cancelled"
            else:
                try:
                    error = str(done.exception() or "")
                except BaseException as exc:
                    error = str(exc)
            if not error:
                return
            with self._lock:
                task = self._tasks.get(key)
                if task is None or task.get("status") in TERMINAL_STATUSES:
                    return
            self._release_task_concurrency(key)
            self._release_task_quota(key)
            self._update_task(key, status=TASK_STATUS_ERROR, error=error, data=[])

        future.add_done_callback(completed)

    def _watch_admission_task(
        self,
        key: str,
        admission: asyncio.Task[Any],
        payload: dict[str, Any] | None = None,
    ) -> None:
        def completed(done: asyncio.Task[Any]) -> None:
            if self._admission_tasks.get(key) is done:
                self._admission_tasks.pop(key, None)
            if done.cancelled():
                error = "scheduler admission task cancelled"
            else:
                try:
                    exception = done.exception()
                except BaseException as exc:
                    exception = exc
                if exception is None:
                    return
                error = str(exception) or "scheduler admission task failed"
            self._cleanup_payload_spool(payload or {})
            with self._lock:
                task = self._tasks.get(key)
                if task is None or task.get("status") in TERMINAL_STATUSES:
                    return
            self._release_task_concurrency(key)
            self._release_task_quota(key)
            self._update_task(key, status=TASK_STATUS_ERROR, error=error, data=[])

        admission.add_done_callback(completed)

    def _run_task(
        self,
        key: str,
        mode: str,
        payload: dict[str, Any],
        identity: dict[str, object],
        model: str,
        *,
        raise_on_error: bool = False,
    ) -> None:
        started = time.time()
        context = current_execution_context()
        account_ref = (
            anonymize_token(context.lease.access_token)
            if context is not None and context.lease is not None
            else ""
        )
        self._update_task(
            key,
            status=TASK_STATUS_RUNNING,
            error="",
            **({"account_ref": account_ref} if account_ref else {}),
        )
        # 创建进度回调，每个步骤完成后更新任务状态
        def progress_callback(step: str, conversation_id: str = "") -> None:
            if step == "image_stream_resolve_start":
                self._update_task(key, started_ts=time.time())
            updates: dict[str, Any] = {"progress": step}
            normalized_conversation_id = _clean(conversation_id)
            if normalized_conversation_id:
                updates["conversation_id"] = normalized_conversation_id
                context = current_execution_context()
                if context is not None and context.lease is not None:
                    updates["account_ref"] = anonymize_token(context.lease.access_token)
            self._update_task(key, **updates)
        payload_with_progress: dict[str, Any] | None = None
        runtime_payload = payload
        try:
            runtime_payload = self._materialize_payload(payload)
            # 将进度回调添加到 payload 中（handler 会提取并传递给 ConversationRequest）
            payload_with_progress = {**runtime_payload, "progress_callback": progress_callback}
            handler = self.edit_handler if mode == "edit" else self.generation_handler
            result = handler(payload_with_progress)
            if not isinstance(result, dict):
                raise RuntimeError("image task returned streaming result unexpectedly")
            data = result.get("data")
            conversation_id = _clean(result.get("_conversation_id") or result.get("conversation_id"))
            account_email = _clean(result.get("_account_email") or result.get("account_email"))
            if not isinstance(data, list) or not data:
                upstream = _clean(result.get("message"))
                if upstream:
                    message = upstream
                else:
                    message = "号池中没有可用账号或所有账号均被限流，请检查号池状态（账号额度、是否被封禁、是否到达生图上限）"
                error = RuntimeError(message)
                if account_email:
                    setattr(error, "account_email", account_email)
                if conversation_id:
                    setattr(error, "conversation_id", conversation_id)
                raise error
            if not self._commit_task_quota(key, data):
                raise QuotaSettlementError("image task quota reservation is no longer active")
            usage = result.get("usage")
            duration_ms = int((time.time() - started) * 1000)
            self._release_task_concurrency(key)
            self._update_task(
                key,
                status=TASK_STATUS_SUCCESS,
                data=data,
                usage=usage,
                error="",
                duration_ms=duration_ms,
                **({"conversation_id": conversation_id} if conversation_id else {}),
            )
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用完成",
                request_preview=request_text(payload.get("prompt")),
                urls=_collect_image_urls(data),
                account_email=account_email,
            )
        except Exception as exc:
            error_message = str(exc) or "image task failed"
            account_email = _clean(getattr(exc, "account_email", ""))
            conversation_id = _clean(getattr(exc, "conversation_id", ""))
            if not conversation_id:
                with self._lock:
                    conversation_id = _clean((self._tasks.get(key) or {}).get("conversation_id"))
            if not _is_resumable_timeout(exc, conversation_id):
                self._release_task_quota(key)
            duration_ms = int((time.time() - started) * 1000)
            self._release_task_concurrency(key)
            self._update_task(key, status=TASK_STATUS_ERROR, error=error_message, data=[],
                              duration_ms=duration_ms,
                              **({"conversation_id": conversation_id} if conversation_id else {}))
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败",
                request_preview=request_text(payload.get("prompt")),
                status="failed",
                error=error_message,
                account_email=account_email,
            )
            if raise_on_error:
                raise
        finally:
            self._release_task_concurrency(key)
            self._cleanup_payload_spool(payload)
            if payload_with_progress is not None:
                payload_with_progress["images"] = []
                payload_with_progress["mask"] = []
                payload_with_progress["markup"] = []
            if runtime_payload is not payload:
                runtime_payload["images"] = []
                runtime_payload["mask"] = []
                runtime_payload["markup"] = []

    def _log_call(
        self,
        identity: dict[str, object],
        mode: str,
        model: str,
        started: float,
        suffix: str,
        *,
        request_preview: str = "",
        status: str = "success",
        error: str = "",
        urls: list[str] | None = None,
        account_email: str = "",
    ) -> None:
        endpoint = "/v1/images/edits" if mode == "edit" else "/v1/images/generations"
        summary_prefix = "图生图" if mode == "edit" else "文生图"
        detail = {
            "key_id": identity.get("id"),
            "key_name": identity.get("name"),
            "role": identity.get("role"),
            "endpoint": endpoint,
            "model": model,
            "started_at": datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": _now_iso(),
            "duration_ms": int((time.time() - started) * 1000),
            "status": status,
        }
        if request_preview:
            detail["request_text"] = request_preview
        if error:
            detail["error"] = error
        if account_email:
            detail["account_email"] = account_email
        if urls:
            detail["urls"] = list(dict.fromkeys(urls))
        try:
            log_service.add(LOG_TYPE_CALL, f"{summary_prefix}{suffix}", detail)
        except Exception:
            pass

    def _update_task(self, key: str, **updates: Any) -> None:
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                return
            task.update(updates)
            task["updated_at"] = _now_iso()
            task["updated_ts"] = time.time()
            if self.state_store is not None:
                if set(updates).issubset({"progress", "started_ts"}):
                    return
                self._save_task_locked(task, synchronous=False)
                if (
                    self.task_writer is not None
                    and self.task_writer.running
                    and ("conversation_id" in updates or task.get("status") in TERMINAL_STATUSES)
                ):
                    try:
                        self.task_writer.flush()
                    except Exception:
                        pass
            else:
                self._save_locked()

    def _load_locked(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        raw_items = raw.get("tasks") if isinstance(raw, dict) else raw
        if not isinstance(raw_items, list):
            return {}
        tasks: dict[str, dict[str, Any]] = {}
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            task_id = _clean(item.get("id"))
            owner = _clean(item.get("owner_id"))
            if not task_id or not owner:
                continue
            status = _clean(item.get("status"))
            if status not in {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING, TASK_STATUS_SUCCESS, TASK_STATUS_ERROR}:
                status = TASK_STATUS_ERROR
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": status,
                "mode": "edit" if item.get("mode") == "edit" else "generate",
                "model": _clean(item.get("model"), "gpt-image-2"),
                "size": _clean(item.get("size")),
                "quality": _clean(item.get("quality"), "auto"),
                "created_at": _clean(item.get("created_at"), _now_iso()),
                "updated_at": _clean(item.get("updated_at"), _clean(item.get("created_at"), _now_iso())),
                "created_ts": item.get("created_ts"),
                "updated_ts": item.get("updated_ts"),
                "started_ts": item.get("started_ts"),
                "duration_ms": item.get("duration_ms"),
            }
            quota_reservation_id = _clean(item.get("quota_reservation_id"))
            if quota_reservation_id:
                task["quota_reservation_id"] = quota_reservation_id
                try:
                    task["quota_requested_units"] = max(1, int(item.get("quota_requested_units") or 1))
                except (TypeError, ValueError, OverflowError):
                    task["quota_requested_units"] = 1
                quota_status = _clean(item.get("quota_status"))
                task["quota_status"] = quota_status or (
                    "committed" if status == TASK_STATUS_SUCCESS else "reserved"
                )
                try:
                    task["quota_committed_units"] = max(0, int(item.get("quota_committed_units") or 0))
                except (TypeError, ValueError, OverflowError):
                    task["quota_committed_units"] = 0
            data = item.get("data")
            if isinstance(data, list):
                task["data"] = data
                task["_persisted_data"] = deepcopy(data)
            usage = item.get("usage")
            if isinstance(usage, dict):
                task["usage"] = usage
            error = _clean(item.get("error"))
            if error:
                task["error"] = error
            tasks[_task_key(owner, task_id)] = task
        return tasks

    def _save_locked(self) -> None:
        if self.state_store is not None:
            for task in self._tasks.values():
                self._save_task_locked(task)
            return
        tasks = sorted(self._tasks.values(), key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        items = []
        for task in tasks:
            item = {
                key: value
                for key, value in task.items()
                if key not in {"data", "_persisted_data"}
            }
            persisted_data = task.get("_persisted_data")
            if isinstance(persisted_data, list):
                item["data"] = persisted_data
            items.append(item)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps({"tasks": items}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(self.path)

    def _save_task_locked(self, task: dict[str, Any], *, synchronous: bool = True) -> None:
        if self.state_store is None:
            self._save_locked()
            return
        record = {
            "task_key": _task_key(str(task.get("owner_id") or ""), str(task.get("id") or "")),
            "owner_id": task.get("owner_id"),
            "task_id": task.get("id"),
            "mode": task.get("mode"),
            "status": task.get("status"),
            "model": task.get("model"),
            "remote_task_id": task.get("conversation_id"),
            "metadata": {
                "size": task.get("size"),
                "quality": task.get("quality"),
                "n": task.get("n", 1),
                "created_at": task.get("created_at"),
                "account_ref": task.get("account_ref"),
                "quota_reservation_id": task.get("quota_reservation_id"),
                "quota_requested_units": task.get("quota_requested_units"),
                "quota_status": task.get("quota_status"),
                "quota_committed_units": task.get("quota_committed_units"),
            },
            "result": _task_result_metadata(task) if task.get("status") == TASK_STATUS_SUCCESS else None,
            "error": task.get("error"),
            "updated_at": task.get("updated_ts") or time.time(),
        }
        if not synchronous and self.task_writer is not None and self.task_writer.running:
            self.task_writer.submit(record)
            return
        if self.task_writer is not None and hasattr(self.task_writer, "save_now"):
            self.task_writer.save_now(record)
            return
        self.state_store.save_task(record)

    def _recover_unfinished_locked(self) -> bool:
        changed = False
        for task in self._tasks.values():
            if task.get("status") in UNFINISHED_STATUSES and not _clean(task.get("conversation_id")):
                self._release_task_quota(_task_key(_clean(task.get("owner_id")), _clean(task.get("id"))))
                task["status"] = TASK_STATUS_ERROR
                task["error"] = (
                    "服务已重启，未完成的图片任务提交结果未知"
                    if self.state_store is not None
                    else "服务已重启，未完成的图片任务已中断"
                )
                task["updated_at"] = _now_iso()
                changed = True
        return changed

    def _cleanup_locked(self) -> bool:
        try:
            retention_days = max(1, int(self.retention_days_getter()))
        except Exception:
            retention_days = 30
        try:
            max_rows = max(100, int(config.image_task_max_rows))
        except Exception:
            max_rows = IMAGE_TASK_MAX_ROWS
        cutoff = time.time() - retention_days * 86400
        if self.state_store is not None:
            cleanup = getattr(self.state_store, "cleanup_tasks", None)
            if callable(cleanup):
                cleanup(
                    now=time.time(),
                    retention_seconds=retention_days * 86400,
                    max_rows=max_rows,
                    task_types=("generate", "edit"),
                )

        removed_keys = {
            key
            for key, task in self._tasks.items()
            if task.get("status") in TERMINAL_STATUSES
            and _timestamp(task.get("updated_ts") or task.get("updated_at")) < cutoff
        }
        remaining_terminal = [
            (key, task)
            for key, task in self._tasks.items()
            if task.get("status") in TERMINAL_STATUSES and key not in removed_keys
        ]
        removed_keys.update(
            key
            for key, _task in sorted(
                remaining_terminal,
                key=lambda item: _timestamp(item[1].get("updated_ts") or item[1].get("updated_at")),
                reverse=True,
            )[max_rows:]
        )
        for key in removed_keys:
            self._tasks.pop(key, None)
        if removed_keys and self.state_store is not None:
            if self.task_writer is not None:
                self.task_writer.delete_tasks(removed_keys)
            else:
                self.state_store.delete_tasks(removed_keys)
        return bool(removed_keys)

    def resume_poll(
        self,
        identity: dict[str, object],
        task_id: str,
        extra_timeout_secs: float = 30.0,
    ) -> dict[str, Any]:
        """恢复对已超时任务的轮询，额外等待 extra_timeout_secs 秒。"""
        from services import sub2_gateway
        if sub2_gateway.enabled():
            sub2_gateway.unsupported("旧网页会话任务恢复；请重新提交生图任务")
        owner = _owner_id(identity)
        key = _task_key(owner, _clean(task_id))
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                raise ValueError("task not found")
            if task.get("status") != TASK_STATUS_ERROR:
                raise ValueError("task is not in error state")
            error_msg = _clean(task.get("error"))
            if "超时" not in error_msg:
                raise ValueError("task error is not a timeout error")
            conversation_id = _clean(task.get("conversation_id"))
            if not conversation_id:
                raise ValueError("task has no conversation_id")
            mode = task.get("mode", "generate")
            model = task.get("model", "gpt-image-2")
            account_ref = _clean(task.get("account_ref"))
            if not self._renew_task_quota(task):
                raise QuotaSettlementError("image task quota reservation is no longer active")
            self._acquire_task_concurrency(key, identity)
            # 将任务状态重置为 running
            try:
                self._update_task(key, status=TASK_STATUS_RUNNING, error="")
            except BaseException:
                self._release_task_concurrency(key)
                raise

        try:
            self._dispatch_resume_poll(
                key,
                conversation_id,
                extra_timeout_secs,
                dict(identity),
                mode,
                model,
                account_ref,
            )
        except BaseException:
            self._release_task_concurrency(key)
            raise
        return _public_task(task)

    async def resume_poll_async(
        self,
        identity: dict[str, object],
        task_id: str,
        extra_timeout_secs: float = 30.0,
    ) -> dict[str, Any]:
        owner = _owner_id(identity)
        key = _task_key(owner, _clean(task_id))
        result = self.resume_poll(identity, task_id, extra_timeout_secs)
        admission = self._admission_tasks.get(key)
        if admission is None:
            return result
        try:
            await admission
        except SchedulerError:
            self._release_task_concurrency(key)
            with self._lock:
                self._update_task(key, status=TASK_STATUS_ERROR, error="scheduler queue is full")
            raise
        except BaseException:
            self._release_task_concurrency(key)
            raise
        return result

    def _dispatch_resume_poll(
        self,
        key: str,
        conversation_id: str,
        extra_timeout_secs: float,
        identity: dict[str, object],
        mode: str,
        model: str,
        account_ref: str = "",
    ) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._fallback_executor.submit(
                self._run_resume_poll,
                key,
                conversation_id,
                extra_timeout_secs,
                identity,
                mode,
                model,
                account_ref=account_ref,
            )
        else:
            admission = asyncio.create_task(
                self._admit_resume_poll(
                    key,
                    conversation_id,
                    extra_timeout_secs,
                    identity,
                    mode,
                    model,
                    account_ref,
                )
            )
            self._admission_tasks[key] = admission
            self._watch_admission_task(key, admission)

    async def _admit_resume_poll(
        self,
        key: str,
        conversation_id: str,
        extra_timeout_secs: float,
        identity: dict[str, object],
        mode: str,
        model: str,
        account_ref: str = "",
    ) -> None:
        if self.scheduler_service is None or not getattr(
            getattr(self.scheduler_service, "settings", None), "enabled", True
        ):
            self._fallback_executor.submit(
                self._run_resume_poll,
                key,
                conversation_id,
                extra_timeout_secs,
                identity,
                mode,
                model,
                account_ref=account_ref,
            )
            return
        payload = {"model": model, "n": 1}
        with self._lock:
            task = self._tasks.get(key) or {}
            account_ref = _clean(task.get("account_ref"))
        requirement = (
            self.scheduler_service.classify_image_edit(payload)
            if mode == "edit"
            else self.scheduler_service.classify_image_generation(payload)
        )
        if account_ref:
            requirement = replace(requirement, account_ref=account_ref)
        future = await self.scheduler_service.admit_background(
            requirement,
            lambda lease, _cancel: self._run_resume_poll(
                key,
                conversation_id,
                extra_timeout_secs,
                identity,
                mode,
                model,
                account_ref=account_ref,
                access_token=lease.access_token,
                raise_on_error=True,
            ),
        )
        self._watch_background_future(key, future)

    def _run_resume_poll(
        self,
        key: str,
        conversation_id: str,
        extra_timeout_secs: float,
        identity: dict[str, object],
        mode: str,
        model: str,
        *,
        account_ref: str = "",
        access_token: str = "",
        raise_on_error: bool = False,
    ) -> None:
        """后台线程：继续轮询已有 conversation_id 的图片结果。"""
        started = time.time()
        backend = None
        fallback_slot = False
        try:
            from services.openai_backend_api import OpenAIBackendAPI
            if not access_token:
                from services.account_service import account_service

                if account_ref:
                    access_token = next(
                        (
                            str(item.get("access_token") or "")
                            for item in account_service.scheduler_snapshot()
                            if anonymize_token(item.get("access_token")) == account_ref
                        ),
                        "",
                    )
                    if not access_token:
                        raise RuntimeError("恢复图片任务时原账号已不可用")
                else:
                    plan_type, _ = split_image_model(model)
                    access_token = account_service.get_available_access_token(
                        plan_type=plan_type,
                        source_type="codex" if is_codex_image_model(model) else None,
                        plan_types=("plus", "team", "pro")
                        if is_codex_image_model(model) and not plan_type
                        else None,
                    )
                    fallback_slot = True
            backend = OpenAIBackendAPI(access_token=access_token)
            file_ids, sediment_ids = backend._poll_image_results(
                conversation_id,
                extra_timeout_secs,
            )
            if not file_ids and not sediment_ids:
                raise RuntimeError(
                    f"继续等待 {extra_timeout_secs} 秒后仍未找到图片结果。"
                )

            image_urls = backend.resolve_conversation_image_urls(
                conversation_id, file_ids, sediment_ids, poll=False,
            )
            if not image_urls:
                raise RuntimeError("图片 URL 解析失败")

            # 异步任务默认返回 URL。保留远端 URL 可让 SQLite 重启恢复继续可用，
            # 同时避免恢复路径下载并重新编码整张图片。
            data = [{"url": url} for url in image_urls]
            if not self._commit_task_quota(key, data):
                raise QuotaSettlementError("image task quota reservation is no longer active")
            self._release_task_concurrency(key)
            self._update_task(key, status=TASK_STATUS_SUCCESS, data=data, error="", duration_ms=int((time.time() - started) * 1000))
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用完成（续轮询）",
                status="success",
                urls=_collect_image_urls(data),
            )
        except ImageRateLimitedError as exc:
            # 续轮询的会话归属原账号，无法换号接管；标记冷却并快速失败，
            # 避免继续占着并发槽位等满 extra_timeout_secs。
            try:
                from services.account_service import account_service as _account_service

                _account_service.mark_image_rate_limited(access_token)
            except Exception:
                pass
            cooldown_secs = int(getattr(exc, "retry_after_secs", 0) or config.image_rate_limit_cooldown_secs)
            error_message = (
                "账号触发图片生成滑动窗口限流（generating images too quickly），"
                f"已冷却约 {max(1, cooldown_secs // 60)} 分钟后自动恢复，请稍后重试。"
            )
            self._release_task_quota(key)
            duration_ms = int((time.time() - started) * 1000)
            self._release_task_concurrency(key)
            self._update_task(
                key,
                status=TASK_STATUS_ERROR,
                error=error_message,
                data=[],
                duration_ms=duration_ms,
                conversation_id=conversation_id,
            )
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败（续轮询）",
                status="failed",
                error=error_message,
            )
            if raise_on_error:
                raise
        except Exception as exc:
            error_message = str(exc) or "resume poll failed"
            if not _is_resumable_timeout(exc, conversation_id):
                self._release_task_quota(key)
            duration_ms = int((time.time() - started) * 1000)
            self._release_task_concurrency(key)
            self._update_task(
                key,
                status=TASK_STATUS_ERROR,
                error=error_message,
                data=[],
                duration_ms=duration_ms,
                conversation_id=conversation_id,
            )
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败（续轮询）",
                status="failed",
                error=error_message,
            )
            if raise_on_error:
                raise
        finally:
            self._release_task_concurrency(key)
            if backend is not None:
                backend.close()
            if fallback_slot:
                try:
                    from services.account_service import account_service

                    account_service.release_image_slot(access_token)
                except Exception:
                    pass


image_task_service = ImageTaskService(
    DATA_DIR / "image_tasks.json",
    scheduler_service_instance=scheduler_service,
    state_store=scheduler_service.state_store,
)
