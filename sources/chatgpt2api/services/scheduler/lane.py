from __future__ import annotations

import asyncio
import heapq
import inspect
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Event as ThreadEvent
from typing import Any, AsyncIterator, Callable

from services.scheduler.errors import (
    NoProviderAvailableError,
    QueueFullError,
    QueueTimeoutError,
    SchedulerDeadlineError,
    SchedulerError,
)
from services.scheduler.context import (
    ExecutionContext,
    install_execution_context,
    reset_execution_context,
)
from services.scheduler.models import AccountRequirement, Lane, Lease, Outcome


logger = logging.getLogger(__name__)


class _StreamDone:
    pass


@dataclass(frozen=True)
class _StreamFailure:
    error: BaseException


class GlobalConcurrencyLimiter:
    """Process-local concurrency budget shared by all scheduler lanes."""

    def __init__(self, limit: int):
        self.limit = max(1, int(limit))
        self.active = 0

    def try_acquire(self, cost: int) -> bool:
        normalized_cost = max(1, int(cost))
        if self.active + normalized_cost > self.limit:
            return False
        self.active += normalized_cost
        return True

    def release(self, cost: int) -> None:
        self.active = max(0, self.active - max(1, int(cost)))


class BoundedStream(AsyncIterator[Any]):
    """An async stream backed by a bounded queue owned by one lane."""

    def __init__(self, lane: "LaneScheduler", job: "_QueuedJob", max_items: int):
        self._lane = lane
        self._job = job
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(1, max_items))
        self._closed = False

    def __aiter__(self) -> "BoundedStream":
        return self

    async def __anext__(self) -> Any:
        if self._closed:
            raise StopAsyncIteration
        item = await self._queue.get()
        if isinstance(item, _StreamDone):
            self._closed = True
            raise StopAsyncIteration
        if isinstance(item, _StreamFailure):
            self._closed = True
            raise item.error
        return item

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._lane._close_stream(self._job)


@dataclass(eq=False)
class _QueuedJob:
    work: Callable[..., Any]
    requirement: AccountRequirement
    cost: int
    priority: int
    sequence: int
    enqueued_at: float
    deadline: float
    queue_deadline: float
    future: asyncio.Future[Any]
    cancel_event: ThreadEvent
    is_stream: bool = False
    stream: BoundedStream | None = None
    started: bool = False
    client_cancelled: bool = False
    execution_context: ExecutionContext | None = None


class LaneScheduler:
    """A bounded async admission queue backed by fixed worker threads."""

    def __init__(
        self,
        lane: Lane,
        settings: Any,
        account_pool: Any,
        *,
        clock: Callable[[], float] = time.monotonic,
        stream_queue_size: int = 8,
        aging_seconds: float = 1.0,
        lease_replacer: Callable[[ExecutionContext, set[str], Outcome], Lease | None] | None = None,
        global_limiter: GlobalConcurrencyLimiter | None = None,
        worker_cleanup: Callable[[_QueuedJob], None] | None = None,
    ):
        self.lane = lane
        self.settings = settings.lanes[lane] if hasattr(settings, "lanes") else settings
        self.account_pool = account_pool
        self._clock = clock
        self._stream_queue_size = max(1, int(stream_queue_size))
        self._aging_seconds = max(0.01, float(aging_seconds))
        self._lease_replacer = lease_replacer
        self._global_limiter = global_limiter
        self._worker_cleanup = worker_cleanup
        self._loop: asyncio.AbstractEventLoop | None = None
        self._condition: asyncio.Condition | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._workers: list[asyncio.Task[None]] = []
        self._queue: list[tuple[int, float, int, _QueuedJob]] = []
        self._sequence = 0
        self._pending_cost = 0
        self._active_cost = 0
        self._active_jobs = 0
        self._started = False
        self._stopping = False
        self._metrics = {
            "submitted": 0,
            "admitted": 0,
            "completed": 0,
            "failed": 0,
            "rejected": 0,
            "timed_out": 0,
            "cancelled": 0,
            "queue_wait_ms": [],
            "peak_active_cost": 0,
            "peak_active_jobs": 0,
        }

    @property
    def running(self) -> bool:
        return self._started and not self._stopping

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        if self._started:
            if self._loop is not loop:
                raise RuntimeError("LaneScheduler cannot be shared across event loops")
            return
        self._loop = loop
        self._condition = asyncio.Condition()
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(self.settings.worker_count)),
            thread_name_prefix=f"scheduler-{self.lane.value}",
        )
        self._stopping = False
        self._started = True
        self._workers = [
            asyncio.create_task(
                self._worker_loop(),
                name=f"scheduler-{self.lane.value}-worker-{index}",
            )
            for index in range(max(1, int(self.settings.worker_count)))
        ]

    async def stop(self) -> None:
        if not self._started:
            return
        condition = self._require_condition()
        async with condition:
            self._stopping = True
            while self._queue:
                _, _, _, job = self._queue.pop()
                self._pending_cost -= job.cost
                self._fail_queued_job(job, SchedulerError("scheduler lane stopped"))
            self._pending_cost = 0
            condition.notify_all()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
        self._workers = []
        self._executor = None
        self._condition = None
        self._loop = None
        self._started = False

    async def submit(
        self,
        work: Callable[..., Any],
        requirement: AccountRequirement,
        *,
        cost: int = 1,
        priority: int = 0,
        deadline: float | None = None,
    ) -> Any:
        job = await self._enqueue(
            work,
            requirement,
            cost=cost,
            priority=priority,
            deadline=deadline,
            is_stream=False,
        )
        try:
            return await asyncio.shield(job.future)
        except asyncio.CancelledError:
            job.client_cancelled = True
            job.cancel_event.set()
            job.future.add_done_callback(self._consume_background_exception)
            condition = self._require_condition()
            async with condition:
                condition.notify_all()
            raise

    async def submit_stream(
        self,
        work: Callable[..., Any],
        requirement: AccountRequirement,
        *,
        cost: int = 1,
        priority: int = 0,
        deadline: float | None = None,
    ) -> BoundedStream:
        job = await self._enqueue(
            work,
            requirement,
            cost=cost,
            priority=priority,
            deadline=deadline,
            is_stream=True,
        )
        assert job.stream is not None
        return job.stream

    async def submit_background(
        self,
        work: Callable[..., Any],
        requirement: AccountRequirement,
        *,
        cost: int = 1,
        priority: int = 0,
        deadline: float | None = None,
    ) -> asyncio.Future[Any]:
        """Admit a job without making the caller wait for its result."""
        job = await self._enqueue(
            work,
            requirement,
            cost=cost,
            priority=priority,
            deadline=deadline,
            is_stream=False,
        )
        job.future.add_done_callback(self._consume_background_exception)
        return job.future

    async def submit_batch(
        self,
        works: list[Callable[..., Any]],
        requirement: AccountRequirement,
        *,
        cost: int = 1,
        priority: int = 0,
        deadline: float | None = None,
    ) -> list[asyncio.Future[Any]]:
        """Atomically admit a bounded set of independent jobs."""
        if not works:
            return []
        await self.start()
        normalized_cost = max(1, int(cost))
        now = self._clock()
        request_deadline = self._request_deadline(now, deadline)
        queue_deadline = min(
            request_deadline,
            now + max(0.001, int(self.settings.queue_wait_timeout_ms) / 1000.0),
        )
        loop = self._require_loop()
        condition = self._require_condition()
        total_cost = normalized_cost * len(works)
        jobs: list[_QueuedJob] = []
        async with condition:
            self._metrics["submitted"] += len(works)
            if self._stopping:
                self._metrics["rejected"] += len(works)
                raise SchedulerError("scheduler lane is stopping")
            provider_error = self._provider_error(requirement)
            if provider_error is not None:
                self._metrics["rejected"] += len(works)
                raise provider_error
            if normalized_cost > int(self.settings.global_concurrency):
                self._metrics["rejected"] += len(works)
                raise QueueFullError(
                    "request cost exceeds lane capacity",
                    retry_after_seconds=self._queue_retry_after(),
                )
            if len(self._queue) + len(works) > int(self.settings.queue_max_items):
                self._metrics["rejected"] += len(works)
                raise QueueFullError(
                    "scheduler queue is full",
                    retry_after_seconds=self._queue_retry_after(),
                )
            if self._pending_cost + self._active_cost + total_cost > int(self.settings.queue_max_cost):
                self._metrics["rejected"] += len(works)
                raise QueueFullError(
                    "scheduler queue cost is full",
                    retry_after_seconds=self._queue_retry_after(),
                )
            normalized_requirement = self._with_cost(requirement, normalized_cost)
            for work in works:
                self._sequence += 1
                job = _QueuedJob(
                    work=work,
                    requirement=normalized_requirement,
                    cost=normalized_cost,
                    priority=int(priority),
                    sequence=self._sequence,
                    enqueued_at=now,
                    deadline=request_deadline,
                    queue_deadline=queue_deadline,
                    future=loop.create_future(),
                    cancel_event=ThreadEvent(),
                )
                jobs.append(job)
                heapq.heappush(
                    self._queue,
                    (-job.priority, job.queue_deadline, job.sequence, job),
                )
            self._pending_cost += total_cost
            self._metrics["admitted"] += len(works)
            condition.notify_all()
        for job in jobs:
            job.future.add_done_callback(
                lambda future, queued_job=job: self._cancel_job_from_future(future, queued_job),
            )
        return [job.future for job in jobs]

    async def wait_until_idle(self, timeout: float = 5.0) -> None:
        condition = self._require_condition()

        async def wait() -> None:
            async with condition:
                while self._queue or self._active_jobs:
                    await condition.wait()

        await asyncio.wait_for(wait(), timeout=max(0.001, timeout))

    def metrics_snapshot(self) -> dict[str, object]:
        queue_waits = list(self._metrics["queue_wait_ms"])
        return {
            "lane": self.lane.value,
            "queued": len(self._queue),
            "queued_cost": self._pending_cost,
            "active_jobs": self._active_jobs,
            "active_cost": self._active_cost,
            "peak_active_cost": self._metrics["peak_active_cost"],
            "peak_active_jobs": self._metrics["peak_active_jobs"],
            "submitted": self._metrics["submitted"],
            "admitted": self._metrics["admitted"],
            "completed": self._metrics["completed"],
            "failed": self._metrics["failed"],
            "rejected": self._metrics["rejected"],
            "timed_out": self._metrics["timed_out"],
            "cancelled": self._metrics["cancelled"],
            "queue_wait_ms": queue_waits,
        }

    async def _enqueue(
        self,
        work: Callable[..., Any],
        requirement: AccountRequirement,
        *,
        cost: int,
        priority: int,
        deadline: float | None,
        is_stream: bool,
    ) -> _QueuedJob:
        await self.start()
        normalized_cost = max(1, int(cost))
        now = self._clock()
        request_deadline = self._request_deadline(now, deadline)
        queue_deadline = min(
            request_deadline,
            now + max(0.001, int(self.settings.queue_wait_timeout_ms) / 1000.0),
        )
        loop = self._require_loop()
        future = loop.create_future()
        self._sequence += 1
        job = _QueuedJob(
            work=work,
            requirement=requirement,
            cost=normalized_cost,
            priority=int(priority),
            sequence=self._sequence,
            enqueued_at=now,
            deadline=request_deadline,
            queue_deadline=queue_deadline,
            future=future,
            cancel_event=ThreadEvent(),
            is_stream=is_stream,
        )
        if is_stream:
            job.stream = BoundedStream(self, job, self._stream_queue_size)

        condition = self._require_condition()
        async with condition:
            self._metrics["submitted"] += 1
            if self._stopping:
                self._metrics["rejected"] += 1
                raise SchedulerError("scheduler lane is stopping")
            provider_error = self._provider_error(requirement)
            if provider_error is not None:
                self._metrics["rejected"] += 1
                raise provider_error
            if normalized_cost > int(self.settings.global_concurrency):
                self._metrics["rejected"] += 1
                raise QueueFullError(
                    "request cost exceeds lane capacity",
                    retry_after_seconds=self._queue_retry_after(),
                )
            if len(self._queue) >= int(self.settings.queue_max_items):
                self._metrics["rejected"] += 1
                raise QueueFullError(
                    "scheduler queue is full",
                    retry_after_seconds=self._queue_retry_after(),
                )
            if (
                self._pending_cost + self._active_cost + normalized_cost
                > int(self.settings.queue_max_cost)
            ):
                self._metrics["rejected"] += 1
                raise QueueFullError(
                    "scheduler queue cost is full",
                    retry_after_seconds=self._queue_retry_after(),
                )
            requirement = self._with_cost(requirement, normalized_cost)
            job.requirement = requirement
            heapq.heappush(
                self._queue,
                (-job.priority, job.queue_deadline, job.sequence, job),
            )
            self._pending_cost += normalized_cost
            self._metrics["admitted"] += 1
            condition.notify_all()
        return job

    async def _worker_loop(self) -> None:
        while True:
            item = await self._take_job()
            if item is None:
                return
            job, lease = item
            await self._execute_job(job, lease)

    async def _take_job(self) -> tuple[_QueuedJob, Lease] | None:
        condition = self._require_condition()
        while True:
            async with condition:
                if self._stopping and not self._queue:
                    return None
                now = self._clock()
                self._prune_expired_locked(now)
                candidates = self._runnable_entries_locked(now)
                if not candidates:
                    await self._wait_for_capacity_locked(now)
                    continue
                for entry in candidates:
                    job = entry[3]
                    self._remove_entry_locked(entry)
                    if job.cancel_event.is_set():
                        self._metrics["cancelled"] += 1
                        self._cancel_queued_job(job)
                        continue
                    provider_error = self._provider_error(job.requirement)
                    if provider_error is not None:
                        self._metrics["failed"] += 1
                        self._fail_queued_job(job, provider_error)
                        continue
                    lease = self.account_pool.try_acquire(
                        job.requirement,
                        deadline=job.deadline,
                    )
                    if lease is None:
                        self._push_entry_locked(entry)
                        continue
                    if self._global_limiter is not None and not self._global_limiter.try_acquire(job.cost):
                        self.account_pool.release(
                            lease,
                            outcome=Outcome.CANCELLED,
                            latency_ms=0.0,
                        )
                        self._push_entry_locked(entry)
                        continue
                    job.started = True
                    self._active_jobs += 1
                    self._active_cost += job.cost
                    self._metrics["peak_active_jobs"] = max(
                        self._metrics["peak_active_jobs"],
                        self._active_jobs,
                    )
                    self._metrics["peak_active_cost"] = max(
                        self._metrics["peak_active_cost"],
                        self._active_cost,
                    )
                    queue_waits = self._metrics["queue_wait_ms"]
                    queue_waits.append(max(0.0, (now - job.enqueued_at) * 1000.0))
                    if len(queue_waits) > 1024:
                        del queue_waits[:-1024]
                    return job, lease
                await self._wait_for_capacity_locked(now)

    async def _execute_job(self, job: _QueuedJob, lease: Lease) -> None:
        loop = self._require_loop()
        executor = self._executor
        if executor is None:
            self._complete_job_error(job, SchedulerError("scheduler executor is unavailable"))
            self._release_active(job, lease, Outcome.SERVER_ERROR, 0.0)
            return
        started_at = self._clock()
        executor_future = loop.run_in_executor(
            executor,
            self._run_sync,
            job,
            lease,
            loop,
        )
        outcome = Outcome.SUCCESS
        try:
            if job.deadline != float("inf"):
                remaining = max(0.0, job.deadline - self._clock())
                result = await asyncio.wait_for(
                    asyncio.shield(executor_future),
                    timeout=remaining,
                )
            else:
                result = await asyncio.shield(executor_future)
            if job.is_stream:
                await self._finish_stream(job, None)
            else:
                self._complete_job_result(job, result)
            self._metrics["completed"] += 1
        except asyncio.TimeoutError:
            outcome = Outcome.TIMEOUT
            self._metrics["timed_out"] += 1
            job.cancel_event.set()
            error = SchedulerDeadlineError("scheduler request deadline exceeded")
            await self._finish_job_error(job, error)
            try:
                await asyncio.shield(executor_future)
            except BaseException:
                pass
        except asyncio.CancelledError:
            outcome = Outcome.CANCELLED
            self._metrics["cancelled"] += 1
            job.cancel_event.set()
            try:
                await asyncio.shield(executor_future)
            except BaseException:
                pass
            await self._finish_job_error(job, SchedulerDeadlineError("scheduler request cancelled"))
        except BaseException as exc:
            outcome = Outcome.SERVER_ERROR
            self._metrics["failed"] += 1
            await self._finish_job_error(job, exc)
        finally:
            self._release_active(
                job,
                lease,
                outcome,
                max(0.0, (self._clock() - started_at) * 1000.0),
            )

    def _run_sync(
        self,
        job: _QueuedJob,
        lease: Lease,
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        context = ExecutionContext(
            lease=lease,
            requirement=job.requirement,
            cancel_event=job.cancel_event,
            replace_lease=self._lease_replacer,
        )
        job.execution_context = context
        context_token = install_execution_context(context)
        try:
            result = self._invoke_work(job.work, lease, job.cancel_event)
            if inspect.isawaitable(result):
                result = asyncio.run(result)
            if not job.is_stream:
                return result
            if hasattr(result, "__aiter__"):
                raise TypeError("stream work must return a synchronous iterable")
            try:
                for item in result:
                    if job.cancel_event.is_set():
                        break
                    put_future = asyncio.run_coroutine_threadsafe(
                        self._put_stream_item(job, item),
                        loop,
                    )
                    try:
                        if not put_future.result():
                            break
                    except BaseException:
                        break
            except TypeError:
                raise TypeError("stream work must return an iterable") from None
            return None
        finally:
            reset_execution_context(context_token)
            self._run_worker_cleanup(job)

    def _run_worker_cleanup(self, job: _QueuedJob) -> None:
        if self._worker_cleanup is None:
            return
        try:
            self._worker_cleanup(job)
        except Exception:
            logger.warning("scheduler_worker_cleanup_failed", exc_info=True)

    @staticmethod
    def _invoke_work(
        work: Callable[..., Any],
        lease: Lease,
        cancel_event: ThreadEvent,
    ) -> Any:
        try:
            signature = inspect.signature(work)
            positional = [
                parameter
                for parameter in signature.parameters.values()
                if parameter.kind
                in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
            accepts_varargs = any(
                parameter.kind == inspect.Parameter.VAR_POSITIONAL
                for parameter in signature.parameters.values()
            )
        except (TypeError, ValueError):
            positional = []
            accepts_varargs = True
        if accepts_varargs or len(positional) >= 2:
            return work(lease, cancel_event)
        if len(positional) == 1:
            return work(lease)
        return work()

    async def _put_stream_item(self, job: _QueuedJob, item: Any) -> bool:
        stream = job.stream
        if stream is None:
            return False
        while not job.cancel_event.is_set() and not stream._closed:
            if job.deadline != float("inf") and self._clock() >= job.deadline:
                job.cancel_event.set()
                return False
            try:
                stream._queue.put_nowait(item)
                return True
            except asyncio.QueueFull:
                wait = 0.01
                if job.deadline != float("inf"):
                    wait = min(wait, max(0.001, job.deadline - self._clock()))
                await asyncio.sleep(wait)
        return False

    async def _finish_stream(self, job: _QueuedJob, error: BaseException | None) -> None:
        stream = job.stream
        if stream is None:
            return
        if stream._closed:
            if not job.future.done():
                job.future.set_result(None)
            return
        terminal: object = _StreamFailure(error) if error is not None else _StreamDone()
        while not stream._closed:
            if job.deadline != float("inf") and self._clock() >= job.deadline:
                job.cancel_event.set()
                stream._closed = True
                break
            try:
                stream._queue.put_nowait(terminal)
                break
            except asyncio.QueueFull:
                wait = 0.01
                if job.deadline != float("inf"):
                    wait = min(wait, max(0.001, job.deadline - self._clock()))
                await asyncio.sleep(wait)
        if not job.future.done():
            job.future.set_result(None)

    async def _finish_job_error(self, job: _QueuedJob, error: BaseException) -> None:
        if job.is_stream:
            await self._finish_stream(job, error)
            return
        self._complete_job_error(job, error)

    async def _close_stream(self, job: _QueuedJob) -> None:
        job.cancel_event.set()
        stream = job.stream
        if stream is not None:
            while True:
                try:
                    stream._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            try:
                stream._queue.put_nowait(_StreamDone())
            except asyncio.QueueFull:
                pass
        condition = self._require_condition()
        async with condition:
            condition.notify_all()

    def _release_active(
        self,
        job: _QueuedJob,
        lease: Lease,
        outcome: Outcome,
        latency_ms: float,
    ) -> None:
        active_lease = lease
        if job.execution_context is not None:
            active_lease = job.execution_context.lease
        if active_lease is not None:
            self.account_pool.release(active_lease, outcome=outcome, latency_ms=latency_ms)
        if self._global_limiter is not None:
            self._global_limiter.release(job.cost)
        self._active_jobs = max(0, self._active_jobs - 1)
        self._active_cost = max(0, self._active_cost - job.cost)
        condition = self._condition
        if condition is not None:
            self._loop.call_soon_threadsafe(self._notify_condition)

    def _notify_condition(self) -> None:
        condition = self._condition
        loop = self._loop
        if condition is None or loop is None:
            return

        async def notify() -> None:
            async with condition:
                condition.notify_all()

        try:
            loop.create_task(notify())
        except RuntimeError:
            return

    def _cancel_job_from_future(
        self,
        future: asyncio.Future[Any],
        job: _QueuedJob,
    ) -> None:
        if not future.cancelled():
            return
        job.client_cancelled = True
        job.cancel_event.set()
        self._notify_condition()

    def _complete_job_result(self, job: _QueuedJob, result: Any) -> None:
        if not job.future.done():
            job.future.set_result(result)

    def _complete_job_error(self, job: _QueuedJob, error: BaseException) -> None:
        if not job.future.done():
            job.future.set_exception(error)

    @staticmethod
    def _consume_background_exception(future: asyncio.Future[Any]) -> None:
        if future.cancelled():
            return
        try:
            future.exception()
        except BaseException:
            pass

    def _fail_queued_job(self, job: _QueuedJob, error: BaseException) -> None:
        if job.is_stream and job.stream is not None:
            try:
                job.stream._queue.put_nowait(_StreamFailure(error))
            except asyncio.QueueFull:
                pass
            if not job.future.done():
                job.future.set_result(None)
            return
        self._complete_job_error(job, error)

    def _cancel_queued_job(self, job: _QueuedJob) -> None:
        if job.is_stream and job.stream is not None:
            job.stream._closed = True
            if not job.future.done():
                job.future.set_result(None)
        elif not job.future.done():
            job.future.cancel()

    def _prune_expired_locked(self, now: float) -> None:
        expired = [
            entry
            for entry in self._queue
            if now >= entry[3].queue_deadline
        ]
        for entry in expired:
            self._remove_entry_locked(entry)
            self._metrics["failed"] += 1
            self._fail_queued_job(
                entry[3],
                QueueTimeoutError(
                    "scheduler queue wait timed out",
                    retry_after_seconds=self._queue_retry_after(),
                ),
            )

    def _runnable_entries_locked(
        self,
        now: float,
    ) -> list[tuple[int, float, int, _QueuedJob]]:
        capacity = int(self.settings.global_concurrency)
        return sorted(
            (
                entry
                for entry in self._queue
                if self._active_cost + entry[3].cost <= capacity
            ),
            key=lambda entry: self._priority_key(entry[3], now),
        )

    def _priority_key(self, job: _QueuedJob, now: float) -> tuple[float, float, int]:
        age = max(0.0, now - job.enqueued_at) / self._aging_seconds
        return (-(job.priority + age), job.queue_deadline, job.sequence)

    async def _wait_for_capacity_locked(self, now: float) -> None:
        condition = self._require_condition()
        timeout = 0.1
        if self._queue:
            timeout = min(
                timeout,
                max(0.001, min(entry[3].queue_deadline for entry in self._queue) - now),
            )
        try:
            await asyncio.wait_for(condition.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    def _remove_entry_locked(self, entry: tuple[int, float, int, _QueuedJob]) -> None:
        self._queue.remove(entry)
        heapq.heapify(self._queue)
        self._pending_cost = max(0, self._pending_cost - entry[3].cost)

    def _push_entry_locked(self, entry: tuple[int, float, int, _QueuedJob]) -> None:
        heapq.heappush(self._queue, entry)
        self._pending_cost += entry[3].cost

    def _require_condition(self) -> asyncio.Condition:
        if self._condition is None:
            raise RuntimeError("LaneScheduler has not started")
        return self._condition

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise RuntimeError("LaneScheduler has not started")
        return self._loop

    def _request_deadline(self, now: float, deadline: float | None) -> float:
        if deadline is not None:
            normalized = float(deadline)
            if not math.isfinite(normalized):
                raise SchedulerDeadlineError("scheduler deadline must be finite")
            if normalized <= now:
                raise SchedulerDeadlineError("scheduler request deadline has expired")
            return normalized
        timeout_ms = max(1, int(getattr(self.settings, "request_timeout_ms", 120000)))
        return now + timeout_ms / 1000.0

    def _provider_error(self, requirement: AccountRequirement) -> NoProviderAvailableError | None:
        provider_status = getattr(self.account_pool, "provider_status", None)
        if callable(provider_status):
            status, retry_after = provider_status(requirement)
            if status in {"missing", "unavailable", "cooldown"}:
                return NoProviderAvailableError(
                    "no provider is available for the requested capability",
                    retry_after_seconds=(
                        max(1.0, float(retry_after))
                        if retry_after is not None
                        else 5.0
                    ),
                )
            return None
        if not self.account_pool.has_capability(requirement):
            return NoProviderAvailableError(
                "no provider is available for the requested capability",
                retry_after_seconds=5,
            )
        return None

    def _queue_retry_after(self) -> float:
        return max(0.1, int(self.settings.queue_wait_timeout_ms) / 1000.0)

    @staticmethod
    def _with_cost(requirement: AccountRequirement, cost: int) -> AccountRequirement:
        return AccountRequirement(
            lane=requirement.lane,
            model=requirement.model,
            plan_type=requirement.plan_type,
            source_type=requirement.source_type,
            cost=cost,
            account_ref=requirement.account_ref,
        )
