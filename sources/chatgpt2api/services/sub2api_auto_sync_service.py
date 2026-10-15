from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable

from services.sub2api_service import (
    Sub2APIConfig,
    Sub2APIImportService,
    Sub2APISyncBusyError,
    _clean,
    list_remote_accounts,
    sub2api_config,
    sub2api_import_service,
)
from utils.log import logger


def _iso_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _epoch_from_iso(value: object) -> float | None:
    clean = _clean(value)
    if not clean:
        return None
    try:
        return datetime.fromisoformat(clean.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class Sub2APIAutoSyncService:
    def __init__(
        self,
        config: Sub2APIConfig,
        import_service: Sub2APIImportService,
        list_accounts: Callable[[dict], list[dict]] = list_remote_accounts,
        *,
        max_workers: int = 8,
    ) -> None:
        self._config = config
        self._imports = import_service
        self._list_accounts = list_accounts
        self._max_workers = max(1, int(max_workers))
        self._wake_event = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._active_lock = threading.Lock()
        self._active_servers: set[str] = set()
        self._coordinator_thread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._stop_event: threading.Event | None = None

    def start(self, stop_event: threading.Event) -> None:
        with self._lifecycle_lock:
            if self._coordinator_thread is not None and self._coordinator_thread.is_alive():
                return
            self._stop_event = stop_event
            self._wake_event.clear()
            self._recover_interrupted_runs(time.time())
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="sub2api-auto-sync",
            )
            self._coordinator_thread = threading.Thread(
                target=self._run_loop,
                name="sub2api-auto-sync-coordinator",
                daemon=True,
            )
            self._coordinator_thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        with self._lifecycle_lock:
            thread = self._coordinator_thread
            executor = self._executor
            if self._stop_event is not None:
                self._stop_event.set()
            self._wake_event.set()
        if thread is not None:
            thread.join(timeout=timeout)
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        with self._lifecycle_lock:
            self._coordinator_thread = None
            self._executor = None

    def wake(self) -> None:
        self._wake_event.set()

    def notify_server_changed(
        self,
        server_id: str,
        *,
        immediate: bool,
        now: float | None = None,
    ) -> None:
        server = self._config.get_server(server_id)
        if server is None:
            return
        interval = int(server.get("auto_sync_interval_minutes") or 0)
        if interval <= 0:
            self._config.set_auto_sync_state(
                server_id,
                status="disabled",
                next_run_at=None,
            )
            self.wake()
            return

        updates: dict[str, object] = {}
        state = server.get("auto_sync") or {}
        if immediate:
            updates["next_run_at"] = _iso_from_epoch(now if now is not None else time.time())
            if state.get("status") == "disabled":
                updates["status"] = "idle"
        elif not state.get("next_run_at"):
            updates["status"] = "idle"
        if updates:
            self._config.set_auto_sync_state(server_id, **updates)
        self.wake()

    def server_deleted(self, _server_id: str) -> None:
        self.wake()

    def run_once(self, server_id: str, *, now: float | None = None) -> bool:
        started_epoch = now if now is not None else time.time()
        server = self._config.get_server(server_id)
        if server is None or int(server.get("auto_sync_interval_minutes") or 0) <= 0:
            return False

        try:
            operation = self._imports.acquire_operation(server_id)
        except Sub2APISyncBusyError:
            self._schedule_next_cycle(server_id, started_epoch)
            self._log_event("sub2api_auto_sync_skipped", server_id, reason="connection_busy")
            return False

        with operation:
            server = self._config.get_server(server_id)
            if server is None or int(server.get("auto_sync_interval_minutes") or 0) <= 0:
                return False

            started_at = _iso_from_epoch(started_epoch)
            self._config.set_auto_sync_state(
                server_id,
                status="running",
                last_started_at=started_at,
                next_run_at=None,
            )
            self._log_event("sub2api_auto_sync_started", server_id)
            try:
                remote_accounts = self._list_accounts(server)
                ids = list(dict.fromkeys(
                    _clean(item.get("id"))
                    for item in remote_accounts
                    if isinstance(item, dict) and _clean(item.get("id"))
                ))
                self._config.set_auto_sync_state(
                    server_id,
                    last_remote_count=len(remote_accounts),
                )
                if not ids:
                    self._mark_success(server_id, started_epoch, None)
                    return True

                job = self._imports.start_import(
                    server,
                    ids,
                    operation_lock_held=True,
                )
                self._config.set_auto_sync_state(
                    server_id,
                    last_import_job_id=_clean(job.get("job_id")),
                )
                result = self._imports.wait_for_job(server_id, job["job_id"])
                failed = max(0, int(result.get("failed") or 0))
                if result.get("status") != "completed" or failed > 0:
                    self._config.set_auto_sync_state(server_id, last_import_failed=failed)
                    raise RuntimeError(self._job_error(result))
                self._mark_success(server_id, time.time(), result)
                self._log_event(
                    "sub2api_auto_sync_completed",
                    server_id,
                    remote_count=len(remote_accounts),
                    failed=0,
                )
                return True
            except Exception as exc:
                self._mark_failure(server_id, time.time(), str(exc) or "automatic sync failed")
                self._log_event(
                    "sub2api_auto_sync_failed",
                    server_id,
                    error=str(exc) or "automatic sync failed",
                )
                return False

    def _run_loop(self) -> None:
        while not self._should_stop():
            now = time.time()
            next_deadline: float | None = None
            for server in self._config.list_servers():
                server_id = _clean(server.get("id"))
                interval = int(server.get("auto_sync_interval_minutes") or 0)
                if not server_id or interval <= 0:
                    continue
                deadline = _epoch_from_iso((server.get("auto_sync") or {}).get("next_run_at"))
                if deadline is None or deadline <= now:
                    if not self._submit_if_capacity(server_id, now):
                        self._schedule_next_cycle(server_id, now, reason="executor_full")
                elif next_deadline is None or deadline < next_deadline:
                    next_deadline = deadline

            wait_for = 1.0
            if next_deadline is not None:
                wait_for = max(0.05, min(wait_for, next_deadline - time.time()))
            self._wake_event.wait(wait_for)
            self._wake_event.clear()

    def _submit_if_capacity(self, server_id: str, now: float) -> bool:
        with self._active_lock:
            if server_id in self._active_servers:
                return False
            if len(self._active_servers) >= self._max_workers:
                return False
            executor = self._executor
            if executor is None:
                return False
            self._active_servers.add(server_id)
            executor.submit(self._run_active, server_id, now)
            return True

    def _run_active(self, server_id: str, now: float) -> None:
        try:
            self.run_once(server_id, now=now)
        finally:
            with self._active_lock:
                self._active_servers.discard(server_id)
            self.wake()

    def _should_stop(self) -> bool:
        return self._stop_event is not None and self._stop_event.is_set()

    def _recover_interrupted_runs(self, now: float) -> None:
        for server in self._config.list_servers():
            server_id = _clean(server.get("id"))
            if not server_id:
                continue
            interval = int(server.get("auto_sync_interval_minutes") or 0)
            state = server.get("auto_sync") or {}
            if interval <= 0:
                self._config.set_auto_sync_state(
                    server_id,
                    status="disabled",
                    next_run_at=None,
                )
            elif state.get("status") == "running":
                self._config.set_auto_sync_state(
                    server_id,
                    status="failed",
                    last_error="previous automatic sync was interrupted by process restart",
                    next_run_at=_iso_from_epoch(now),
                )

    def _schedule_next_cycle(
        self,
        server_id: str,
        now: float,
        *,
        reason: str | None = None,
    ) -> None:
        server = self._config.get_server(server_id)
        if server is None:
            return
        interval = int(server.get("auto_sync_interval_minutes") or 0)
        if interval <= 0:
            return
        self._config.set_auto_sync_state(
            server_id,
            next_run_at=_iso_from_epoch(now + interval * 60),
        )
        if reason:
            self._log_event("sub2api_auto_sync_skipped", server_id, reason=reason)

    def _mark_success(self, server_id: str, now: float, job: dict | None) -> None:
        server = self._config.get_server(server_id)
        if server is None:
            return
        interval = int(server.get("auto_sync_interval_minutes") or 0)
        state = server.get("auto_sync") or {}
        updates: dict[str, object] = {
            "status": "completed" if interval > 0 else "disabled",
            "last_finished_at": _iso_from_epoch(now),
            "last_success_at": _iso_from_epoch(now),
            "last_error": "",
            "last_import_failed": 0,
        }
        if interval > 0:
            updates["next_run_at"] = state.get("next_run_at") or _iso_from_epoch(now + interval * 60)
        else:
            updates["next_run_at"] = None
        if job is not None:
            updates.update({
                "last_import_job_id": _clean(job.get("job_id")),
                "last_import_failed": max(0, int(job.get("failed") or 0)),
            })
        self._config.set_auto_sync_state(server_id, **updates)

    def _mark_failure(self, server_id: str, now: float, error: str) -> None:
        server = self._config.get_server(server_id)
        if server is None:
            return
        interval = int(server.get("auto_sync_interval_minutes") or 0)
        state = server.get("auto_sync") or {}
        self._config.set_auto_sync_state(
            server_id,
            status="failed" if interval > 0 else "disabled",
            last_finished_at=_iso_from_epoch(now),
            last_error=error[:500],
            next_run_at=(
                state.get("next_run_at")
                or (_iso_from_epoch(now + interval * 60) if interval > 0 else None)
            ),
        )

    @staticmethod
    def _job_error(job: dict) -> str:
        failed = max(0, int(job.get("failed") or 0))
        errors = job.get("errors") if isinstance(job.get("errors"), list) else []
        first_error = errors[0].get("error") if errors and isinstance(errors[0], dict) else ""
        return first_error or f"automatic import failed for {failed} account(s)"

    @staticmethod
    def _log_event(event: str, server_id: str, **details: object) -> None:
        logger.info({"event": event, "server_id": server_id, **details})


sub2api_auto_sync_service = Sub2APIAutoSyncService(
    sub2api_config,
    sub2api_import_service,
)
