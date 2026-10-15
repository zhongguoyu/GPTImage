from __future__ import annotations

import json
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import BoundedSemaphore
from typing import Any
from urllib.parse import quote

from services.account_service import account_service
from services.config import DATA_DIR, config
from services.content_filter import request_text
from services.log_service import LOG_TYPE_CALL, log_service
from services.openai_backend_api import EDITABLE_FILE_MODEL, OpenAIBackendAPI
from services.scheduler.errors import QueueFullError
from utils.helper import new_uuid

TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_ERROR = "error"
UNFINISHED_STATUSES = {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}
TERMINAL_STATUSES = {TASK_STATUS_SUCCESS, TASK_STATUS_ERROR, "completed", "failed", "cancelled"}
EDITABLE_FILE_PLAN_TYPES = ("Plus", "Team", "Pro", "Enterprise")
EDITABLE_FILE_ROOT = DATA_DIR / "files"
EDITABLE_FILE_TASKS_PATH = DATA_DIR / "editable_file_tasks.json"
EDITABLE_FILE_SPOOL_DIR = DATA_DIR / ".editable_file_task_spool"


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean(value: object, default: str = "") -> str:
    return str(value or default).strip()


def _owner_id(identity: dict[str, object]) -> str:
    return _clean(identity.get("id")) or "anonymous"


def _task_key(owner_id: str, task_id: str) -> str:
    return f"{owner_id}:{task_id}"


def _elapsed_seconds(task: dict[str, Any]) -> int:
    start = float(task.get("started_ts") or task.get("created_ts") or 0)
    end = float(task.get("ended_ts") or time.time())
    return max(0, int(end - start)) if start else 0


def _file_url(path: Path, base_url: str) -> str:
    rel = path.resolve().relative_to(EDITABLE_FILE_ROOT.resolve()).as_posix()
    prefix = str(base_url or "").strip().rstrip("/")
    return f"{prefix}/files/{quote(rel, safe='/')}" if prefix else f"/files/{quote(rel, safe='/')}"


def _editable_access_token() -> str:
    accounts = [
        item for item in account_service.list_accounts()
        if _clean(item.get("access_token"))
           and item.get("status") not in {"禁用", "异常"}
           and account_service._account_matches_any_plan_type(item, EDITABLE_FILE_PLAN_TYPES)
    ]
    if not accounts:
        raise RuntimeError("no available plus/team/pro account")
    accounts.sort(key=lambda item: _clean(item.get("last_used_at")))
    token = _clean(accounts[0].get("access_token"))
    return account_service.refresh_access_token(token, event="editable_file_task") or token


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    item = {
        "id": task.get("id"),
        "taskId": task.get("id"),
        "status": task.get("status"),
        "kind": task.get("kind"),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
        "elapsed_seconds": _elapsed_seconds(task),
    }
    for key in ("result", "error"):
        if task.get(key):
            item[key] = task[key]
    return item


class EditableFileTaskService:
    def __init__(
        self,
        path: Path = EDITABLE_FILE_TASKS_PATH,
        *,
        state_store: Any | None = None,
        max_workers: int | None = None,
        queue_max_items: int | None = None,
        spool_dir: Path | None = None,
    ) -> None:
        self.path = path
        self.state_store = state_store
        if self.state_store is None and Path(path).resolve() == EDITABLE_FILE_TASKS_PATH.resolve():
            try:
                from services.scheduler.service import scheduler_service

                self.state_store = scheduler_service.state_store
            except Exception:
                self.state_store = None
        worker_count = max(1, int(max_workers if max_workers is not None else config.file_task_max_workers))
        queue_size = max(0, int(queue_max_items if queue_max_items is not None else config.file_task_queue_max_items))
        self.spool_dir = Path(spool_dir) if spool_dir is not None else EDITABLE_FILE_SPOOL_DIR
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self._cleanup_spool_files()
        self._executor = ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="editable-file-task",
        )
        self._admission = BoundedSemaphore(worker_count + queue_size)
        self._admitted: set[str] = set()
        self._lock = threading.RLock()
        self._tasks: dict[str, dict[str, Any]] = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._tasks = self._load_state_store_locked() if self.state_store is not None else self._load_locked()
            self._cleanup_tasks_locked()
            if self._recover_unfinished_locked():
                self._save_locked()

    def submit_ppt(self, identity: dict[str, object], *, client_task_id: str = "", prompt: str = "", base64_images: list[str] | None = None, base_url: str = "") -> dict[str, Any]:
        return self._submit(identity, client_task_id=client_task_id, kind="ppt", prompt=prompt, base64_images=base64_images or [], base_url=base_url)

    def submit_psd(self, identity: dict[str, object], *, client_task_id: str = "", prompt: str = "", base64_images: list[str] | None = None, base_url: str = "") -> dict[str, Any]:
        return self._submit(identity, client_task_id=client_task_id, kind="psd", prompt=prompt, base64_images=base64_images or [], base_url=base_url)

    def list_tasks(self, identity: dict[str, object], task_ids: list[str]) -> dict[str, Any]:
        owner = _owner_id(identity)
        requested = [_clean(item) for item in task_ids if _clean(item)]
        with self._lock:
            self._cleanup_tasks_locked()
            if requested:
                items = []
                for task_id in requested:
                    task = self._tasks.get(_task_key(owner, task_id))
                    if task is None and self.state_store is not None:
                        record = self.state_store.get_task(f"file:{owner}:{task_id}")
                        converted = self._task_from_state_record(record) if record is not None else None
                        task = converted[1] if converted is not None else None
                    if task is not None:
                        items.append(task)
                found_ids = {str(item.get("id") or "") for item in items}
                return {
                    "items": [_public_task(item) for item in items],
                    "missing_ids": [task_id for task_id in requested if task_id not in found_ids],
                }
            task_by_key = {
                key: task
                for key, task in self._tasks.items()
                if task.get("owner_id") == owner
            }
            if self.state_store is not None:
                try:
                    max_rows = max(1, int(config.file_task_max_rows))
                except (TypeError, ValueError):
                    max_rows = 10000
                for record in self.state_store.list_tasks(
                    owner_id=owner,
                    limit=max_rows,
                    task_types=("editable_ppt", "editable_psd"),
                ):
                    converted = self._task_from_state_record(record)
                    if converted is not None:
                        task_by_key.setdefault(converted[0], converted[1])
            items = list(task_by_key.values())
        items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return {"items": [_public_task(item) for item in items], "missing_ids": []}

    def shutdown(self, *, wait: bool = False) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=not wait)

    def _submit(self, identity: dict[str, object], *, client_task_id: str, kind: str, prompt: str, base64_images: list[str], base_url: str) -> dict[str, Any]:
        from services import sub2_gateway
        if sub2_gateway.enabled():
            sub2_gateway.unsupported("PPT/PSD 文件导出")
        task_id = _clean(client_task_id) or new_uuid()
        owner = _owner_id(identity)
        key = _task_key(owner, task_id)
        now = _now_iso()
        with self._lock:
            if key in self._tasks:
                return _public_task(self._tasks[key])
            if self.state_store is not None:
                record = self.state_store.get_task(f"file:{owner}:{task_id}")
                converted = self._task_from_state_record(record) if record is not None else None
                if converted is not None:
                    existing_task = converted[1]
                    if existing_task.get("status") in UNFINISHED_STATUSES:
                        self._tasks[key] = existing_task
                    return _public_task(existing_task)
            if not self._admission.acquire(blocking=False):
                raise QueueFullError("editable file task queue is full")
            ts = time.time()
            self._tasks[key] = {"id": task_id, "owner_id": owner, "status": TASK_STATUS_QUEUED, "kind": kind, "model": EDITABLE_FILE_MODEL, "created_at": now, "updated_at": now, "created_ts": ts, "updated_ts": ts}
            task = dict(self._tasks[key])
            self._admitted.add(key)
            spool_paths: list[Path] = []
            try:
                spool_paths = self._spool_inputs(base64_images)
                self._save_task_locked(task)
                future = self._executor.submit(
                    self._run_task,
                    key,
                    kind,
                    prompt,
                    tuple(spool_paths),
                    dict(identity),
                    base_url,
                )
                future.add_done_callback(
                    lambda completed, task_key=key, paths=tuple(spool_paths): self._on_future_done(
                        task_key,
                        paths,
                        completed,
                    )
                )
            except Exception:
                self._release_spooled_inputs(spool_paths)
                self._admitted.discard(key)
                self._admission.release()
                self._tasks.pop(key, None)
                self._delete_task_locked(key)
                raise
        return _public_task(task)

    def _run_task(
        self,
        key: str,
        kind: str,
        prompt: str,
        spool_paths: tuple[Path, ...],
        identity: dict[str, object],
        base_url: str,
    ) -> None:
        started = time.time()
        token = ""
        account_email = ""
        base64_images: list[str] = []
        backend = None
        try:
            self._update_task(key, status=TASK_STATUS_RUNNING, error="", started_ts=started)
            base64_images = [path.read_text(encoding="utf-8") for path in spool_paths]
            if kind == "psd" and not base64_images:
                raise ValueError("base64_images is empty")
            token = _editable_access_token()
            account = account_service.get_account(token) or {}
            account_email = _clean(account.get("email"))
            backend = OpenAIBackendAPI(token)
            output_dir = EDITABLE_FILE_ROOT / kind / key.rsplit(":", 1)[-1]
            result = backend.export_psd_zip(base64_images, prompt, output_dir) if kind == "psd" else backend.export_ppt_zip(base64_images, prompt, output_dir)
            account_service.mark_text_used(token)
            data = {"conversation_id": result.conversation_id, "primary_url": _file_url(result.primary_path, base_url), "zip_url": _file_url(result.zip_path, base_url)}
            self._update_task(key, status=TASK_STATUS_SUCCESS, result=data, account_email=account_email, error="", ended_ts=time.time())
            self._log_call(identity, kind, started, request_text(prompt), account_email=account_email, result=data)
        except Exception as exc:
            error = str(exc) or "editable file task failed"
            self._update_task(key, status=TASK_STATUS_ERROR, error=error, account_email=account_email, ended_ts=time.time())
            self._log_call(identity, kind, started, request_text(prompt), status="failed", error=error, account_email=account_email)
        finally:
            if backend is not None:
                backend.close()
            base64_images.clear()
            self._release_spooled_inputs(spool_paths)
            self._release_admission(key)

    def _on_future_done(self, key: str, spool_paths: tuple[Path, ...], future: Any) -> None:
        try:
            if future.cancelled():
                self._mark_future_failure(key, "任务已取消")
            else:
                exception = future.exception()
                if exception is not None:
                    self._mark_future_failure(key, f"任务执行失败: {exception}")
        except Exception:
            # Cleanup must not make a completed Future callback fail noisily.
            pass
        finally:
            self._release_spooled_inputs(spool_paths)
            self._release_admission(key)

    def _mark_future_failure(self, key: str, error: str) -> None:
        with self._lock:
            task = self._tasks.get(key)
            if task is None or task.get("status") in TERMINAL_STATUSES:
                return
        self._update_task(
            key,
            status=TASK_STATUS_ERROR,
            error=error,
            ended_ts=time.time(),
        )

    def _release_admission(self, key: str) -> None:
        with self._lock:
            if key not in self._admitted:
                return
            self._admitted.remove(key)
            self._admission.release()

    def _spool_inputs(self, base64_images: list[str]) -> list[Path]:
        paths: list[Path] = []
        try:
            for image in base64_images:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    dir=self.spool_dir,
                    prefix="task-",
                    suffix=".b64",
                    delete=False,
                ) as handle:
                    handle.write(str(image))
                    paths.append(Path(handle.name))
            return paths
        except Exception:
            self._release_spooled_inputs(paths)
            raise

    @staticmethod
    def _release_spooled_inputs(paths: Any) -> None:
        for path in paths or ():
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                pass

    def _cleanup_spool_files(self) -> None:
        for path in self.spool_dir.glob("task-*.b64"):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

    def _cleanup_tasks_locked(self) -> None:
        retention_seconds = config.file_task_retention_days * 24 * 60 * 60
        max_rows = config.file_task_max_rows
        task_types = ("editable_ppt", "editable_psd")
        now = time.time()
        cutoff = now - retention_seconds
        if self.state_store is not None:
            cleanup = getattr(self.state_store, "cleanup_tasks", None)
            if callable(cleanup):
                cleanup(
                    now=now,
                    retention_seconds=retention_seconds,
                    max_rows=max_rows,
                    task_types=task_types,
                )
            self._prune_local_terminal_tasks_locked(cutoff, max_rows)
            return

        changed = self._prune_local_terminal_tasks_locked(cutoff, max_rows)
        if changed:
            self._save_locked()

    def _prune_local_terminal_tasks_locked(self, cutoff: float, max_rows: int) -> bool:
        changed = False
        terminal = [
            (key, task)
            for key, task in self._tasks.items()
            if task.get("status") in TERMINAL_STATUSES
        ]
        for key, task in terminal:
            updated_ts = float(task.get("updated_ts") or 0)
            if updated_ts < cutoff:
                self._tasks.pop(key, None)
                changed = True
        remaining_terminal = [
            (key, task)
            for key, task in self._tasks.items()
            if task.get("status") in TERMINAL_STATUSES
        ]
        for key, _task in sorted(
            remaining_terminal,
            key=lambda item: float(item[1].get("updated_ts") or 0),
            reverse=True,
        )[max_rows:]:
            self._tasks.pop(key, None)
            changed = True
        if changed:
            return True
        return False

    def public_file_path(self, relative_path: str) -> Path:
        raw = str(relative_path or "").replace("\\", "/").lstrip("/")
        path = (EDITABLE_FILE_ROOT / raw).resolve()
        path.relative_to(EDITABLE_FILE_ROOT.resolve())
        if not path.is_file():
            raise FileNotFoundError(raw)
        return path

    def _update_task(self, key: str, **updates: Any) -> None:
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                return
            task.update(updates)
            task["updated_at"] = _now_iso()
            task["updated_ts"] = time.time()
            self._save_task_locked(task)

    def _load_state_store_locked(self) -> dict[str, dict[str, Any]]:
        self.state_store.initialize()
        has_migration_marker = getattr(self.state_store, "has_migration_marker", None)
        if self.path.exists() and (not callable(has_migration_marker) or not has_migration_marker()):
            self._import_legacy_file_tasks_locked()
        tasks: dict[str, dict[str, Any]] = {}
        for record in self.state_store.load_recoverable_tasks(
            task_types=("editable_ppt", "editable_psd"),
        ):
            converted = self._task_from_state_record(record)
            if converted is not None:
                tasks[converted[0]] = converted[1]
        return tasks

    @staticmethod
    def _task_from_state_record(record: dict[str, object]) -> tuple[str, dict[str, Any]] | None:
        task_id = _clean(record.get("task_id"))
        owner = _clean(record.get("owner_id"))
        if not task_id or not owner:
            return None
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        status = _clean(record.get("status"), TASK_STATUS_ERROR)
        if status == "unknown":
            status = TASK_STATUS_ERROR
        task = {
            "id": task_id,
            "owner_id": owner,
            "status": status,
            "kind": "psd" if record.get("mode") == "editable_psd" else "ppt",
            "model": _clean(record.get("model"), EDITABLE_FILE_MODEL),
            "created_at": _clean(metadata.get("created_at"), _now_iso()),
            "updated_at": _clean(metadata.get("updated_at"), _now_iso()),
            "created_ts": float(record.get("created_at") or 0),
            "updated_ts": float(record.get("updated_at") or 0),
        }
        result = record.get("result")
        if isinstance(result, dict):
            task["result"] = result
        if record.get("error"):
            task["error"] = _clean(record.get("error"))
        elif status == TASK_STATUS_ERROR:
            task["error"] = "服务已重启，未完成的任务已中断"
        return _task_key(owner, task_id), task

    def _import_legacy_file_tasks_locked(self) -> None:
        for item in self._load_locked().values():
            self._save_task_locked(item)

    def _save_task_locked(self, task: dict[str, Any]) -> None:
        if self.state_store is None:
            self._save_locked()
            return
        owner = _clean(task.get("owner_id"))
        task_id = _clean(task.get("id"))
        self.state_store.save_task({
            "task_key": f"file:{owner}:{task_id}",
            "owner_id": owner,
            "task_id": task_id,
            "mode": f"editable_{'psd' if task.get('kind') == 'psd' else 'ppt'}",
            "status": task.get("status"),
            "model": task.get("model") or EDITABLE_FILE_MODEL,
            "metadata": {
                "created_at": task.get("created_at"),
                "updated_at": task.get("updated_at"),
            },
            "result": task.get("result"),
            "error": task.get("error"),
            "created_at": task.get("created_ts"),
            "updated_at": task.get("updated_ts"),
        })

    def _delete_task_locked(self, key: str) -> None:
        if self.state_store is None:
            self._save_locked()
            return
        owner, _, task_id = str(key).partition(":")
        self.state_store.delete_task(f"file:{owner}:{task_id}")

    def _load_locked(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        tasks: dict[str, dict[str, Any]] = {}
        for item in (raw.get("tasks") if isinstance(raw, dict) else raw) or []:
            if not isinstance(item, dict):
                continue
            task_id = _clean(item.get("id"))
            owner = _clean(item.get("owner_id"))
            if not task_id or not owner:
                continue
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": _clean(item.get("status"), TASK_STATUS_ERROR),
                "kind": "psd" if item.get("kind") == "psd" else "ppt",
                "created_at": _clean(item.get("created_at"), _now_iso()),
                "updated_at": _clean(item.get("updated_at"), _clean(item.get("created_at"), _now_iso())),
                "created_ts": float(item.get("created_ts") or 0),
                "updated_ts": float(item.get("updated_ts") or 0),
            }
            for field in ("result", "error", "started_ts", "ended_ts"):
                if item.get(field):
                    task[field] = item[field]
            tasks[_task_key(owner, task_id)] = task
        return tasks

    def _save_locked(self) -> None:
        if self.state_store is not None:
            self.state_store.save_task_batch(
                {
                    "task_key": f"file:{_clean(task.get('owner_id'))}:{_clean(task.get('id'))}",
                    "owner_id": task.get("owner_id"),
                    "task_id": task.get("id"),
                    "mode": f"editable_{'psd' if task.get('kind') == 'psd' else 'ppt'}",
                    "status": task.get("status"),
                    "model": task.get("model") or EDITABLE_FILE_MODEL,
                    "metadata": {
                        "created_at": task.get("created_at"),
                        "updated_at": task.get("updated_at"),
                    },
                    "result": task.get("result"),
                    "error": task.get("error"),
                    "created_at": task.get("created_ts"),
                    "updated_at": task.get("updated_ts"),
                }
                for task in self._tasks.values()
            )
            return
        items = sorted(self._tasks.values(), key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps({"tasks": items}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(self.path)

    def _recover_unfinished_locked(self) -> bool:
        changed = False
        for task in self._tasks.values():
            if task.get("status") in UNFINISHED_STATUSES:
                task["status"] = TASK_STATUS_ERROR
                task["error"] = "服务已重启，未完成的任务已中断"
                task["ended_ts"] = time.time()
                task["updated_at"] = _now_iso()
                task["updated_ts"] = time.time()
                changed = True
        return changed

    def _log_call(
            self,
            identity: dict[str, object],
            kind: str,
            started: float,
            request_preview: str,
            *,
            status: str = "success",
            error: str = "",
            account_email: str = "",
            result: dict[str, str] | None = None,
    ) -> None:
        detail = {
            "key_id": identity.get("id"),
            "key_name": identity.get("name"),
            "role": identity.get("role"),
            "endpoint": f"/v1/{kind}/generations",
            "model": EDITABLE_FILE_MODEL,
            "started_at": datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": _now_iso(),
            "duration_ms": int((time.time() - started) * 1000),
            "status": status,
        }
        if request_preview:
            detail["request_text"] = request_preview
        if account_email:
            detail["account_email"] = account_email
        if error:
            detail["error"] = error
        if result:
            detail["result"] = result
        try:
            log_service.add(LOG_TYPE_CALL, f"{kind.upper()}生成任务{'失败' if status == 'failed' else '完成'}", detail)
        except Exception:
            pass


editable_file_task_service = EditableFileTaskService()
