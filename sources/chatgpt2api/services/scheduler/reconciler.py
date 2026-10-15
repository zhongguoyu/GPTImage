from __future__ import annotations

from threading import Event, RLock, Thread
from typing import Any


class AccountReconciler:
    """Refresh the in-memory account index only when account state changes."""

    def __init__(
        self,
        account_service: Any,
        account_pool: Any,
        *,
        interval_seconds: float = 1.0,
        stop_event: Event | None = None,
    ):
        self.account_service = account_service
        self.account_pool = account_pool
        self.interval_seconds = max(0.001, float(interval_seconds))
        self._stop_event = stop_event or Event()
        self._owns_stop_event = stop_event is None
        self._reconcile_lock = RLock()
        self._last_generation: int | None = None
        self._thread: Thread | None = None

    @property
    def last_generation(self) -> int | None:
        return self._last_generation

    def reconcile_now(self) -> None:
        with self._reconcile_lock:
            observed_generation = self.account_service.scheduler_state_generation()
            snapshot = self.account_service.scheduler_snapshot()
            self.account_pool.reconcile(snapshot)
            latest_generation = self.account_service.scheduler_state_generation()
            self._last_generation = (
                observed_generation
                if latest_generation != observed_generation
                else latest_generation
            )

    def reconcile_if_changed(self) -> bool:
        with self._reconcile_lock:
            generation = self.account_service.scheduler_state_generation()
            if self._last_generation == generation:
                return False
            snapshot = self.account_service.scheduler_snapshot()
            self.account_pool.reconcile(snapshot)
            latest_generation = self.account_service.scheduler_state_generation()
            self._last_generation = generation if latest_generation != generation else latest_generation
            return True

    def start(self) -> None:
        with self._reconcile_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._owns_stop_event:
                self._stop_event.clear()
            self.reconcile_now()
            self._thread = Thread(
                target=self._run,
                name="account-scheduler-reconciler",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._thread = None

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self.reconcile_if_changed()
