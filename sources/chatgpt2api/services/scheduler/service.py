from __future__ import annotations

import asyncio
import ctypes
import time
from pathlib import Path
from threading import Event, Thread
from typing import Any, Callable

from services.account_service import AccountService, account_service
from services.config import config
from services.scheduler.account_pool import AccountPool
from services.scheduler.context import ExecutionContext
from services.scheduler.errors import QueueFullError
from services.scheduler.lane import BoundedStream, GlobalConcurrencyLimiter, LaneScheduler
from services.scheduler.models import AccountRequirement, Lane, Lease, Outcome, SchedulerSettings
from services.scheduler.reconciler import AccountReconciler
from services.scheduler.state_store import SchedulerStateStore, SnapshotWriter, TaskStateWriter
from utils.helper import is_codex_image_model, split_image_model


_MALLOC_TRIM: Callable[[int], object] | None = None
_MALLOC_TRIM_LOADED = False


def _close_current_thread_resource(resource: object) -> None:
    close_current_thread = getattr(resource, "close_current_thread", None)
    if callable(close_current_thread):
        close_current_thread()


def _get_malloc_trim() -> Callable[[int], object] | None:
    global _MALLOC_TRIM, _MALLOC_TRIM_LOADED
    if _MALLOC_TRIM_LOADED:
        return _MALLOC_TRIM
    _MALLOC_TRIM_LOADED = True
    try:
        libc = ctypes.CDLL("libc.so.6")
        malloc_trim = getattr(libc, "malloc_trim", None)
        if callable(malloc_trim):
            _MALLOC_TRIM = malloc_trim
    except Exception:
        _MALLOC_TRIM = None
    return _MALLOC_TRIM


def _malloc_trim() -> None:
    malloc_trim = _get_malloc_trim()
    if malloc_trim is None:
        return
    try:
        malloc_trim(0)
    except Exception:
        return


class SchedulerService:
    """Own the process-local account scheduler and its lifecycle."""

    def __init__(
        self,
        account_service_instance: AccountService | Any = account_service,
        *,
        settings: SchedulerSettings | None = None,
        state_store: SchedulerStateStore | None = None,
        model_router: Callable[[str], object] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.account_service = account_service_instance
        self.settings = settings or config.get_scheduler_settings()
        self._clock = clock
        self._stop_event = Event()
        self.state_store = state_store or SchedulerStateStore(config.app_db_path)
        self.account_pool = AccountPool(
            self.account_service.scheduler_snapshot(),
            self.settings,
            model_router=model_router or self._default_model_router,
            clock=clock,
        )
        self.reconciler = AccountReconciler(
            self.account_service,
            self.account_pool,
            interval_seconds=self.settings.account_reconcile_interval_ms / 1000.0,
            stop_event=self._stop_event,
        )
        self.snapshot_writer = SnapshotWriter(
            self.state_store,
            interval_seconds=self.settings.snapshot_flush_interval_ms / 1000.0,
        )
        self.task_writer = TaskStateWriter(
            self.state_store,
            interval_seconds=self.settings.snapshot_flush_interval_ms / 1000.0,
        )
        self._global_limiter = GlobalConcurrencyLimiter(self.settings.system_concurrency)
        self._lanes = {
            lane: LaneScheduler(
                lane,
                self.settings,
                self.account_pool,
                clock=clock,
                lease_replacer=self._replace_execution_lease,
                global_limiter=self._global_limiter,
                worker_cleanup=self._cleanup_worker_thread,
            )
            for lane in Lane
        }
        self._cooldown_thread: Thread | None = None
        self._watchdog_thread: Thread | None = None
        self._started = False
        self._lifecycle_lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self._started

    def lane(self, lane: Lane) -> LaneScheduler:
        return self._lanes[lane]

    def _cleanup_worker_thread(self, _job: object) -> None:
        resources = [
            self.state_store,
            getattr(self.account_service, "storage", None),
        ]
        try:
            from services.quota_service import quota_service
        except Exception:
            quota_service = None
        resources.append(quota_service)

        seen: set[int] = set()
        for resource in resources:
            if resource is None or id(resource) in seen:
                continue
            seen.add(id(resource))
            _close_current_thread_resource(resource)
        job_lane = getattr(getattr(_job, "requirement", None), "lane", None)
        if job_lane in (Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT):
            _malloc_trim()

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._started:
                return
            if not self.settings.enabled:
                return
            self._stop_event.clear()
            self.state_store.initialize()
            self.account_pool.restore_health(self.state_store.list_health())
            for lane in self._lanes.values():
                await lane.start()
            self.snapshot_writer.start()
            self.task_writer.start()
            self._cooldown_thread = self._start_thread(
                "scheduler-cooldown",
                self._cooldown_loop,
            )
            self._watchdog_thread = self._start_thread(
                "scheduler-lease-watchdog",
                self._watchdog_loop,
            )
            self.reconciler.start()
            self._started = True

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if not self._started:
                return
            self._stop_event.set()
            self.reconciler.stop(timeout=1)
            for lane in self._lanes.values():
                await lane.stop()
            self.task_writer.stop(timeout=1)
            self.snapshot_writer.stop(timeout=1)
            for thread in (self._cooldown_thread, self._watchdog_thread):
                if thread is not None and thread.is_alive():
                    thread.join(timeout=1)
            self._cooldown_thread = None
            self._watchdog_thread = None
            self.state_store.close()
            self._started = False

    def reconcile_now(self) -> None:
        self.reconciler.reconcile_now()

    async def run_text(
        self,
        requirement: AccountRequirement,
        work: Callable[..., Any],
        *,
        priority: int = 0,
        deadline: float | None = None,
    ) -> Any:
        return await self.lane(Lane.TEXT).submit(
            work,
            requirement,
            cost=1,
            priority=priority,
            deadline=deadline,
        )

    async def run_image(
        self,
        requirement: AccountRequirement,
        work: Callable[..., Any],
        *,
        priority: int = 0,
        deadline: float | None = None,
    ) -> Any:
        if requirement.lane not in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
            raise ValueError("run_image requires an image lane")
        return await self.lane(requirement.lane).submit(
            work,
            requirement,
            cost=max(1, requirement.cost),
            priority=priority,
            deadline=deadline,
        )

    async def run_image_units(
        self,
        requirement: AccountRequirement,
        work_factory: Callable[[int], Any],
        *,
        priority: int = 0,
        deadline: float | None = None,
    ) -> list[Any]:
        """Run one bounded lane job per requested image output."""
        if requirement.lane not in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
            raise ValueError("run_image_units requires an image lane")
        futures = await self.submit_image_units(
            requirement,
            work_factory,
            priority=priority,
            deadline=deadline,
        )
        try:
            completed = await asyncio.gather(
                *(asyncio.shield(future) for future in futures),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            for future in futures:
                future.cancel()
            await asyncio.gather(
                *(asyncio.shield(future) for future in futures),
                return_exceptions=True,
            )
            raise

        results: list[Any] = []
        errors: list[BaseException] = []
        for item in completed:
            if isinstance(item, BaseException):
                errors.append(item)
            else:
                results.append(item)
        if not results and errors:
            raise errors[0]
        return results

    async def submit_image_units(
        self,
        requirement: AccountRequirement,
        work_factory: Callable[[int], Any],
        *,
        priority: int = 0,
        deadline: float | None = None,
    ) -> list[asyncio.Future[Any]]:
        """Atomically admit one bounded lane job per requested output."""
        if requirement.lane not in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
            raise ValueError("submit_image_units requires an image lane")
        count = max(1, int(requirement.cost))
        lane_settings = self.settings.lanes[requirement.lane]
        max_batch = max(
            1,
            min(int(lane_settings.queue_max_items), int(lane_settings.queue_max_cost)),
        )
        if count > max_batch:
            raise QueueFullError(
                "image output count exceeds scheduler batch capacity",
                retry_after_seconds=1,
            )
        unit_requirement = AccountRequirement(
            lane=requirement.lane,
            model=requirement.model,
            plan_type=requirement.plan_type,
            source_type=requirement.source_type,
            cost=1,
            account_ref=requirement.account_ref,
        )

        works = [
            (lambda _lease, _cancel, index=index: work_factory(index))
            for index in range(1, count + 1)
        ]
        return await self.lane(requirement.lane).submit_batch(
            works,
            unit_requirement,
            cost=1,
            priority=priority,
            deadline=deadline,
        )

    async def admit_background(
        self,
        requirement: AccountRequirement,
        work: Callable[..., Any],
        *,
        priority: int = 0,
        deadline: float | None = None,
    ) -> asyncio.Future[Any]:
        """Admit a background image job and return its completion future."""
        if requirement.lane not in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
            raise ValueError("admit_background requires an image lane")
        return await self.lane(requirement.lane).submit_background(
            work,
            requirement,
            cost=max(1, requirement.cost),
            priority=priority,
            deadline=deadline,
        )

    async def run_stream(
        self,
        lane: Lane,
        requirement: AccountRequirement,
        work: Callable[..., Any],
        *,
        cost: int = 1,
        priority: int = 0,
        deadline: float | None = None,
    ) -> BoundedStream:
        if lane != requirement.lane:
            raise ValueError("stream lane does not match requirement")
        return await self.lane(lane).submit_stream(
            work,
            requirement,
            cost=max(1, int(cost)),
            priority=priority,
            deadline=deadline,
        )

    def classify_text(self, model: str | None = None) -> AccountRequirement:
        return AccountRequirement(Lane.TEXT, str(model or "auto").strip() or "auto")

    def classify_image_generation(self, payload: dict[str, object]) -> AccountRequirement:
        return self._classify_image(payload, Lane.IMAGE_GENERATION)

    def classify_image_edit(self, payload: dict[str, object]) -> AccountRequirement:
        return self._classify_image(payload, Lane.IMAGE_EDIT)

    def metrics_snapshot(self) -> dict[str, object]:
        health = []
        for item in self.account_pool.snapshot_health():
            redacted = dict(item)
            redacted["account_ref"] = str(redacted.pop("account_key", ""))
            health.append(redacted)
        return {
            "enabled": bool(self.settings.enabled),
            "running": self._started,
            "lanes": {
                lane.value: self._lanes[lane].metrics_snapshot()
                for lane in Lane
            },
            "account_health": health,
        }

    def _classify_image(self, payload: dict[str, object], lane: Lane) -> AccountRequirement:
        requested_model = str(payload.get("model") or "gpt-image-2").strip() or "gpt-image-2"
        plan_type, model = split_image_model(requested_model)
        try:
            cost = max(1, int(payload.get("n") or 1))
        except (TypeError, ValueError):
            cost = 1
        return AccountRequirement(
            lane,
            model or requested_model,
            plan_type=plan_type,
            source_type=(
                str(payload.get("source_type")).strip()
                if payload.get("source_type")
                else ("codex" if is_codex_image_model(requested_model) else None)
            ),
            cost=cost,
        )

    @staticmethod
    def _default_model_router(model: str) -> object:
        # Only consult the already-warmed catalog. A refresh performs network
        # I/O and must stay outside the request admission path.
        try:
            from services.model_service import model_catalog_service

            route = model_catalog_service.cached_route_for_model(model)
            return route if route is not None else set()
        except Exception:
            return set()

    def _replace_execution_lease(
        self,
        context: ExecutionContext,
        excluded_tokens: set[str],
        outcome: Outcome,
    ) -> Lease | None:
        current = context.lease
        if current is not None:
            self.account_pool.release(current, outcome=outcome, latency_ms=0.0)
            context.lease = None
            context.lease_released = True
        replacement = self.account_pool.try_acquire(
            context.requirement,
            deadline=current.deadline if current is not None else self._clock() + 1,
            excluded=excluded_tokens,
        )
        if replacement is None:
            return None
        context.lease = replacement
        context.lease_released = False
        return replacement

    def _start_thread(self, name: str, target: Callable[[], None]) -> Thread:
        thread = Thread(target=target, name=name, daemon=True)
        thread.start()
        return thread

    def _cooldown_loop(self) -> None:
        interval = max(0.05, self.settings.lease_watchdog_interval_ms / 1000.0)
        while not self._stop_event.wait(interval):
            self.account_pool.promote_due_cooldowns()

    def _watchdog_loop(self) -> None:
        interval = max(0.05, self.settings.lease_watchdog_interval_ms / 1000.0)
        while not self._stop_event.wait(interval):
            # Python threads cannot be force-stopped safely. The lane owns
            # active leases and releases them only after the worker observes
            # cancellation and exits; reclaiming here could reuse a token
            # while the old upstream request is still using it.
            for snapshot in self.account_pool.snapshot_health():
                self.snapshot_writer.submit(snapshot)


scheduler_service = SchedulerService()


__all__ = ["SchedulerService", "scheduler_service"]
