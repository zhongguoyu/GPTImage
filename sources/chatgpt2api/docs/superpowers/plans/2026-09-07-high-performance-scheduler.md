
# High-Performance Account Scheduler Implementation Plan

> For agentic workers: REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Replace round-robin account selection, indefinite image-slot waits, and thread-per-task execution with a bounded single-process scheduler for text, image generation, and image editing.

**Architecture:** Add a scheduler package with capability-indexed account heaps, per-account leases, per-capability circuit breakers, three bounded lanes, and fixed executors. Keep hot-path state in memory. Persist coalesced account health snapshots and recoverable image-task metadata in a separate SQLite database. Integrate through the existing protocol handlers and logging wrapper without changing public API paths.

**Tech Stack:** Python 3.13, FastAPI, asyncio, ThreadPoolExecutor, stdlib sqlite3, existing unittest/pytest tests, existing JSON account storage.

---

## File Map

Create these focused modules:

- services/scheduler/__init__.py: export the process-local scheduler singleton and public lane types.
- services/scheduler/models.py: lane names, account requirements, runtime snapshots, leases, stream sentinels, and request metadata.
- services/scheduler/errors.py: overload, provider availability, deadline, and scheduler-state exceptions with HTTP status metadata.
- services/scheduler/account_pool.py: capability indexes, ready/cooldown heaps, load scoring, leases, and circuit state.
- services/scheduler/lane.py: bounded priority queues, fixed executors, weighted capacity, aging, cancellation, and stream bridging.
- services/scheduler/state_store.py: SQLite schema, health snapshots, task metadata, and coalesced writes.
- services/scheduler/service.py: lifecycle, reconciliation, lane construction, execution adapters, and metrics.

Create these tests:

- test/test_scheduler_config.py
- test/test_scheduler_account_pool.py
- test/test_scheduler_reconciler.py
- test/test_scheduler_lane.py
- test/test_scheduler_state_store.py
- test/test_scheduler_service.py
- test/test_scheduler_text_integration.py
- test/test_scheduler_image_integration.py
- test/test_scheduler_async_tasks.py
- test/test_scheduler_integration.py

Modify:

- services/config.py and config.json
- services/account_service.py
- services/log_service.py
- services/protocol/conversation.py
- services/protocol/openai_v1_chat_complete.py
- services/protocol/openai_v1_response.py
- services/protocol/anthropic_v1_messages.py
- services/protocol/openai_v1_image_generations.py
- services/protocol/openai_v1_image_edit.py
- services/image_task_service.py
- api/image_tasks.py
- api/app.py
- api/system.py
- existing account, image, task, and protocol tests

Leave account storage backends, openai_backend_api.py, web UI pages, and PPT/PSD scheduling unchanged unless a focused integration test proves a narrow adapter is required.

## Task 1: Add Scheduler Configuration and Error Contracts

Files:

- Create: services/scheduler/__init__.py
- Create: services/scheduler/models.py
- Create: services/scheduler/errors.py
- Modify: services/config.py
- Modify: config.json
- Create: test/test_scheduler_config.py

- [ ] Step 1: Write failing configuration tests.

Test temporary config loading, safe defaults, invalid-value normalization, and lane capacity clamping:

~~~python
def test_scheduler_defaults_are_bounded_and_exposed(self):
    settings = ConfigStore(temp_config_path).get_scheduler_settings()
    self.assertTrue(settings.enabled)
    self.assertGreater(settings.system_concurrency, 0)
    self.assertGreater(settings.lanes[Lane.TEXT].worker_count, 0)
    self.assertGreater(settings.lanes[Lane.IMAGE_GENERATION].queue_max_cost, 0)

def test_invalid_scheduler_values_use_safe_bounds(self):
    write_config({"scheduler": {
        "system_concurrency": 0,
        "text": {"worker_count": -3, "queue_max_items": "bad"},
    }})
    settings = ConfigStore(temp_config_path).get_scheduler_settings()
    self.assertEqual(settings.system_concurrency, 1)
    self.assertEqual(settings.lanes[Lane.TEXT].worker_count, 1)
    self.assertGreaterEqual(settings.lanes[Lane.TEXT].queue_max_items, 1)

def test_lane_capacity_cannot_exceed_system_capacity(self):
    write_config({"scheduler": {
        "system_concurrency": 4,
        "text": {"global_concurrency": 100},
    }})
    settings = ConfigStore(temp_config_path).get_scheduler_settings()
    self.assertLessEqual(settings.lanes[Lane.TEXT].global_concurrency, 4)
~~~

Use the existing config-test pattern: patch CONFIG_FILE, isolate each test in TemporaryDirectory, and restore module-level config state.

- [ ] Step 2: Run the focused tests and verify the contract is missing.

Run:

~~~bash
.venv/bin/python -m pytest test/test_scheduler_config.py -q
~~~

Expected: import or attribute failures because the scheduler package and get_scheduler_settings do not exist.

- [ ] Step 3: Implement the immutable model and error types.

Define:

~~~python
class Lane(str, Enum):
    TEXT = "text"
    IMAGE_GENERATION = "image_generation"
    IMAGE_EDIT = "image_edit"

@dataclass(frozen=True)
class LaneSettings:
    worker_count: int
    global_concurrency: int
    per_account_concurrency: int
    queue_max_items: int
    queue_max_cost: int
    queue_wait_timeout_ms: int

@dataclass(frozen=True)
class SchedulerSettings:
    enabled: bool
    system_concurrency: int
    lanes: Mapping[Lane, LaneSettings]
    image_total_per_account_concurrency: int
    account_reconcile_interval_ms: int
    lease_watchdog_interval_ms: int
    snapshot_flush_interval_ms: int

class Outcome(str, Enum):
    SUCCESS = "success"
    RATE_LIMIT = "rate_limit"
    AUTH_ERROR = "auth_error"
    NETWORK_ERROR = "network_error"
    SERVER_ERROR = "server_error"
    CANCELLED = "cancelled"

class ErrorClass(str, Enum):
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    NETWORK = "network"
    TIMEOUT = "timeout"
    SERVER = "server"

@dataclass(frozen=True)
class AccountRequirement:
    lane: Lane
    model: str
    plan_type: str | None = None
    source_type: str | None = None
    cost: int = 1

@dataclass(frozen=True)
class Lease:
    lease_id: str
    access_token: str
    account_key: str
    lane: Lane
    cost: int
    deadline: float
    token_generation: int
~~~

Define SchedulerError, QueueFullError(status_code 429), QueueTimeoutError(status_code 429), NoProviderAvailableError(status_code 503), and SchedulerDeadlineError(status_code 504). Each error carries a short public message and optional retry_after_seconds without an access token.

- [ ] Step 4: Add config normalization and safe defaults.

Add DEFAULT_SCHEDULER in services/config.py with separate text, image_generation, and image_edit dictionaries. Each lane includes per_account_concurrency, and the scheduler includes image_total_per_account_concurrency. Add get_scheduler_settings() using the existing normalization helpers. Clamp each lane global capacity to system_concurrency, ensure worker, per-account, and queue values are at least one, and clamp reconcile/watchdog/flush intervals to positive values.

Add the same shape to config.json with conservative starting values. Map the existing image_account_concurrency setting to image_total_per_account_concurrency when the new field is absent, so existing deployments retain the current per-account image limit. Do not calculate any value from account count.

- [ ] Step 5: Run tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_config.py -q
git add services/scheduler services/config.py config.json test/test_scheduler_config.py
git commit -m "feat: add scheduler configuration contract"
~~~

Expected: all configuration normalization and error metadata tests pass before the commit.

## Task 2: Implement Capability-Indexed Account Leases

Files:

- Create: services/scheduler/account_pool.py
- Modify: services/scheduler/models.py
- Create: test/test_scheduler_account_pool.py

- [ ] Step 1: Write failing account-pool tests.

Use deterministic fake account dictionaries, an injected model router, and a monotonic test clock:

~~~python
def test_selects_only_accounts_matching_text_model(self):
    pool = AccountPool(make_accounts("free", "pro"), settings, model_router=fake_router)
    lease = pool.try_acquire(
        AccountRequirement(Lane.TEXT, "pro-only"),
        deadline=clock.now + 5,
    )
    self.assertEqual(lease.account_key, "pro")

def test_selection_does_not_scan_all_accounts(self):
    pool = AccountPool(make_accounts_for_count(2000), settings, model_router=fake_router)
    pool.reset_selection_probe_count()
    for _ in range(100):
        lease = pool.try_acquire(requirement, deadline=clock.now + 5)
        pool.release(lease, outcome=Outcome.SUCCESS, latency_ms=10)
    self.assertLessEqual(pool.selection_probe_count, 500)

def test_account_capacity_is_enforced(self):
    pool = AccountPool(make_accounts("one"), settings, model_router=fake_router)
    leases = [
        pool.try_acquire(image_requirement, deadline=clock.now + 5)
        for _ in range(2)
    ]
    self.assertIsNone(pool.try_acquire(image_requirement, deadline=clock.now + 5))
    pool.release(leases[0], outcome=Outcome.SUCCESS, latency_ms=20)
    self.assertIsNotNone(pool.try_acquire(image_requirement, deadline=clock.now + 5))

def test_image_breaker_does_not_remove_text_capability(self):
    pool = AccountPool(make_accounts("shared"), settings, model_router=fake_router)
    lease = pool.try_acquire(image_requirement, deadline=clock.now + 5)
    pool.release(lease, outcome=Outcome.SERVER_ERROR, latency_ms=1000)
    pool.open_breaker("shared", Lane.IMAGE_GENERATION)
    self.assertIsNotNone(pool.try_acquire(text_requirement, deadline=clock.now + 5))
    self.assertIsNone(pool.try_acquire(image_requirement, deadline=clock.now + 5))
~~~

Also assert that image cost 4 consumes four permits and that excluded accounts are never selected.

- [ ] Step 2: Run focused tests and verify the implementation is absent.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_account_pool.py -q
~~~

Expected: import or constructor failures for AccountPool and Outcome.

- [ ] Step 3: Implement account runtime state and capability indexes.

Implement:

~~~python
class AccountPool:
    def __init__(self, accounts, settings, *, model_router, clock=time.monotonic): ...
    def reconcile(self, accounts: Iterable[dict[str, object]]) -> None: ...
    def try_acquire(self, requirement, *, deadline: float, excluded=None) -> Lease | None: ...
    def release(self, lease, *, outcome, latency_ms: float) -> None: ...
    def record_failure(self, account_key, lane, error_class) -> None: ...
    def record_success(self, account_key, lane, latency_ms: float) -> None: ...
    def promote_due_cooldowns(self) -> int: ...
    def reclaim_expired(self, now: float) -> list[Lease]: ...
    def snapshot_health(self) -> list[dict[str, object]]: ...
~~~

Store one AccountRuntime per access-token key. Build indexes by text model capability and image model/plan/source capability. Use heap revision numbers so removal, status changes, quota changes, and breaker transitions invalidate old entries without scanning a heap.

Use the approved score:

~~~python
score = (
    0.45 * lane_load_ratio
    + 0.25 * total_load_ratio
    + 0.20 * normalized_ewma_latency
    + 0.10 * recent_failure_penalty
    + jitter
)
~~~

Hold one RLock only around counter and heap mutations. try_acquire must never sleep; return None for no capacity, no eligible candidate, open breaker, or excluded account.

- [ ] Step 4: Implement release, breaker, and cooldown behavior.

Maintain closed, open, and half_open state per account and lane. Use different penalties for rate limits, authentication failures, network failures, timeouts, and server errors. Use exponential cooldown with random jitter and one probe permit in half-open state.

The background cooldown timer calls promote_due_cooldowns. Request selection may discard only a bounded number of stale heap entries from the top. Duplicate release must be idempotent and cannot make counters negative.

- [ ] Step 5: Run tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_account_pool.py -q
git add services/scheduler/models.py services/scheduler/account_pool.py test/test_scheduler_account_pool.py
git commit -m "feat: add capability indexed account leases"
~~~

Expected: routing, capacity, breaker, cooldown, lease-release, duplicate-release, and scale-probe tests pass.

## Task 3: Connect AccountService State Without Hot-Path Scans

Files:

- Modify: services/account_service.py
- Create: services/scheduler/reconciler.py
- Create: test/test_scheduler_reconciler.py
- Modify: test/test_account_image_capabilities.py

- [ ] Step 1: Write failing generation and snapshot tests.

~~~python
service = AccountService(JSONStorageBackend(path))
before = service.scheduler_state_generation()
service.add_account_items([{
    "access_token": "token-1",
    "status": "正常",
    "type": "Plus",
    "source_type": "web",
    "quota": 3,
}])
after = service.scheduler_state_generation()
self.assertGreater(after, before)
snapshot = service.scheduler_snapshot()
self.assertEqual(snapshot[0]["access_token"], "token-1")
self.assertEqual(snapshot[0]["quota"], 3)
~~~

Test that a reconciler calls scheduler_snapshot only after the generation changes and that status/quota changes remove a token from the ready heap after reconciliation.

- [ ] Step 2: Run focused tests and verify the seam is missing.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_reconciler.py test/test_account_image_capabilities.py -q
~~~

Expected: missing scheduler_state_generation and scheduler_snapshot failures.

- [ ] Step 3: Add AccountService generation and internal snapshot.

Initialize _scheduler_state_generation after accounts load. Increment it once in _save_accounts after a successful storage write. Expose:

~~~python
def scheduler_state_generation(self) -> int:
    with self._lock:
        return self._scheduler_state_generation

def scheduler_snapshot(self) -> list[dict[str, object]]:
    with self._lock:
        return [dict(item) for item in self._accounts.values()]
~~~

Do not add raw tokens to list_accounts API responses; scheduler_snapshot is an internal process-local seam.

- [ ] Step 4: Implement generation-driven reconciliation.

Create AccountReconciler with reconcile_if_changed and reconcile_now. Store the last generation, call scheduler_snapshot only when needed, and pass the snapshot to AccountPool.reconcile. The periodic loop sleeps using account_reconcile_interval_ms and stops through the application Event.

The periodic loop may rebuild O(N) only when the generation changes. No request path may call scheduler_snapshot or list_accounts.

- [ ] Step 5: Run tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_reconciler.py test/test_account_image_capabilities.py -q
git add services/account_service.py services/scheduler/reconciler.py test/test_scheduler_reconciler.py test/test_account_image_capabilities.py
git commit -m "feat: expose account state for scheduler reconciliation"
~~~

Expected: generation and change-driven heap tests pass with existing account capability tests.

## Task 4: Add Bounded Lanes and Fixed Worker Execution

Files:

- Create: services/scheduler/lane.py
- Modify: services/scheduler/models.py
- Create: test/test_scheduler_lane.py

- [ ] Step 1: Write failing lane and stream-bridge tests.

~~~python
async def test_queue_full_returns_429_without_starting_a_thread():
    lane = make_lane(queue_max_items=1, worker_count=1)
    first = asyncio.create_task(lane.submit(blocking_job, requirement, cost=1))
    await lane.wait_until_active()
    second = asyncio.create_task(lane.submit(blocking_job, requirement, cost=1))
    with pytest.raises(QueueFullError):
        await lane.submit(blocking_job, requirement, cost=1)
    self.assertEqual(executor.submitted_count, 1)
    first.cancel()
    second.cancel()

async def test_cost_four_uses_four_global_units():
    lane = make_lane(global_concurrency=4)
    task = asyncio.create_task(lane.submit(job, requirement, cost=4))
    await lane.wait_until_active()
    with pytest.raises(QueueFullError):
        await lane.submit(job, requirement, cost=1)
    release_job.set()
    await task

async def test_stream_close_releases_lease():
    stream = await lane.submit_stream(streaming_job, requirement, cost=1)
    await anext(stream)
    await stream.aclose()
    await lane.wait_until_idle()
    assert pool.released_all_leases()
~~~

Use IsolatedAsyncioTestCase where practical. Assert waiting items do not create threads; only the fixed executor workers start.

- [ ] Step 2: Run focused tests and verify the lane is absent.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_lane.py -q
~~~

Expected: import failures for LaneScheduler and BoundedStream.

- [ ] Step 3: Implement the bounded priority queue and weighted capacity.

Implement the async API:

~~~python
class LaneScheduler:
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def submit(self, work, requirement, *, cost=1, priority=0, deadline=None): ...
    async def submit_stream(self, work, requirement, *, cost=1, priority=0, deadline=None): ...
    def metrics_snapshot(self) -> dict[str, object]: ...
~~~

Use an event-loop-owned heap keyed by effective priority, enqueue sequence, and deadline. Maintain item count and total cost. Reject before enqueue when either limit is exceeded. Use one ThreadPoolExecutor(max_workers=settings.worker_count) per lane.

The dispatcher obtains an account lease through AccountPool.try_acquire before submitting a job. If account capacity is temporarily unavailable, wait on a release event or cooldown deadline; never block an arbitrary request thread or busy-loop.

- [ ] Step 4: Implement stream bridging and cancellation.

Use a bounded per-request queue with done and error sentinels. The worker blocks only against the bounded queue, so a slow client cannot grow memory without limit. Closing the consumer sets a cancellation event; the worker stops consuming upstream, releases the lease, and records cancellation.

On deadline, cancel the future, release the lease exactly once, and complete the caller with SchedulerDeadlineError. Keep release in the worker finally block even when a cancellation callback also runs.

- [ ] Step 5: Run tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_lane.py -q
git add services/scheduler/models.py services/scheduler/lane.py test/test_scheduler_lane.py
git commit -m "feat: add bounded scheduler lanes"
~~~

Expected: queue, weighted capacity, aging, cancellation, fixed-worker, and stream backpressure tests pass.

## Task 5: Add SQLite Scheduler Snapshots and Task Metadata

Files:

- Create: services/scheduler/state_store.py
- Create: test/test_scheduler_state_store.py

- [ ] Step 1: Write failing SQLite tests.

~~~python
def test_health_snapshot_upsert_is_idempotent(self):
    store = SchedulerStateStore(db_path)
    store.initialize()
    store.upsert_health({
        "account_key": "a",
        "lane": "text",
        "breaker_state": "open",
    })
    store.upsert_health({
        "account_key": "a",
        "lane": "text",
        "breaker_state": "half_open",
    })
    rows = store.list_health()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0]["breaker_state"], "half_open")

def test_unknown_submission_is_not_retried_after_restart(self):
    store = SchedulerStateStore(db_path)
    store.initialize()
    store.save_task({
        "task_key": "owner:task",
        "status": "unknown",
        "remote_task_id": None,
    })
    restored = store.load_recoverable_tasks()
    self.assertEqual(restored[0]["status"], "unknown")
    self.assertFalse(restored[0]["retryable"])
~~~

Use a temporary database. Test WAL and busy_timeout, concurrent health upserts, coalesced writes, and failure preservation.

- [ ] Step 2: Run focused tests and verify the store is missing.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_state_store.py -q
~~~

Expected: import failure for SchedulerStateStore.

- [ ] Step 3: Implement the SQLite schema and connection policy.

Use data/scheduler.db, separate from data/logs.db. Create:

~~~sql
CREATE TABLE IF NOT EXISTS scheduler_account_health (
    account_key TEXT NOT NULL,
    lane TEXT NOT NULL,
    breaker_state TEXT NOT NULL,
    cooldown_until REAL,
    quota_snapshot INTEGER,
    ewma_latency_ms REAL NOT NULL DEFAULT 0,
    recent_success_count INTEGER NOT NULL DEFAULT 0,
    recent_failure_count INTEGER NOT NULL DEFAULT 0,
    last_error_class TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL,
    PRIMARY KEY (account_key, lane)
);

CREATE TABLE IF NOT EXISTS scheduler_tasks (
    task_key TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    model TEXT NOT NULL,
    remote_task_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT,
    error TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL
);
~~~

Use thread-local connections, WAL, synchronous=NORMAL, busy_timeout=5000, parameterized SQL, and short transactions.

- [ ] Step 4: Implement coalesced writes and recovery classification.

Implement SnapshotWriter with a thread-safe map keyed by account and lane. New health values replace older pending values. Flush on interval, pending-count threshold, and scheduler shutdown. Do not write queue depth, active counters, leases, or poll heartbeats per request.

Implement save_task, update_task, and load_recoverable_tasks. Only tasks with a known remote task ID are eligible for poll recovery. Unknown queued/running submissions are returned as unknown and non-retryable. Never persist raw input images or request bodies.

Add import_legacy_tasks(path) for data/image_tasks.json: parse the existing tasks list, insert metadata with INSERT OR IGNORE in bounded batches, preserve task ownership and IDs, and rename the source to image_tasks.json.migrated only after the final transaction commits. If a migrated name exists, append a timestamp suffix. A failed import leaves the original file untouched and can be retried on the next startup.

- [ ] Step 5: Run tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_state_store.py -q
git add services/scheduler/state_store.py test/test_scheduler_state_store.py
git commit -m "feat: persist scheduler health and task state"
~~~

Expected: schema, WAL, upsert, coalescing, failure-isolation, task metadata, and recovery tests pass.

## Task 6: Build Scheduler Service and Application Lifecycle

Files:

- Create: services/scheduler/service.py
- Modify: services/scheduler/__init__.py
- Modify: api/app.py
- Modify: api/system.py
- Create: test/test_scheduler_service.py

- [ ] Step 1: Write failing lifecycle and routing tests.

~~~python
async def test_start_and_stop_manage_all_lanes(self):
    service = make_scheduler(fake_accounts)
    await service.start()
    self.assertTrue(service.lane(Lane.TEXT).running)
    self.assertTrue(service.lane(Lane.IMAGE_GENERATION).running)
    self.assertTrue(service.lane(Lane.IMAGE_EDIT).running)
    await service.stop()
    self.assertFalse(service.lane(Lane.TEXT).running)

async def test_classifier_assigns_image_edit_lane_and_cost(self):
    request = service.classify_image_edit({"model": "gpt-image-2", "n": 4})
    self.assertEqual(request.lane, Lane.IMAGE_EDIT)
    self.assertEqual(request.cost, 4)
~~~

- [ ] Step 2: Run focused tests and verify lifecycle code is missing.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_service.py -q
~~~

Expected: import or constructor failures for SchedulerService.

- [ ] Step 3: Implement the service facade and singleton.

Implement:

~~~python
class SchedulerService:
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def reconcile_now(self) -> None: ...
    async def run_text(self, requirement, work, *, priority=0, deadline=None): ...
    async def run_image(self, requirement, work, *, priority=0, deadline=None): ...
    async def run_stream(self, lane, requirement, work, *, cost=1, priority=0, deadline=None): ...
    def metrics_snapshot(self) -> dict[str, object]: ...
~~~

Construct the account pool, reconciler, three lanes, health writer, cooldown timer, lease watchdog, and metrics registry. start and stop are idempotent. The singleton performs no network calls at import time.

- [ ] Step 4: Wire application lifespan and protected metrics.

Start the scheduler after log_service.initialize and before the account watcher. Stop it before the application lifespan exits, after setting the shared stop event. Preserve existing backup, log cleanup, and image cleanup behavior.

Add GET /api/system/scheduler, protected by the existing identity/role checks, returning lane metrics and redacted account health. Never return tokens, request bodies, or image data.

- [ ] Step 5: Run tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_service.py test/test_system_api.py -q
git add services/scheduler services/account_service.py api/app.py api/system.py test/test_scheduler_service.py test/test_system_api.py
git commit -m "feat: add scheduler service lifecycle"
~~~

Expected: lifecycle, protected metrics, routing, and shutdown tests pass.

## Task 7: Schedule Text Requests Without Breaking Streams or Cache Dedupe

Files:

- Modify: services/log_service.py
- Modify: services/protocol/conversation.py
- Modify: services/protocol/openai_v1_chat_complete.py
- Modify: services/protocol/openai_v1_response.py
- Modify: services/protocol/anthropic_v1_messages.py
- Modify: api/ai.py
- Create: test/test_scheduler_text_integration.py
- Modify: test/test_text_model_routing.py and text protocol tests

- [ ] Step 1: Add failing text scheduling tests.

Cover all text routes, model capability routing, lease release, invalid-token retry before first output, no replay after partial output, and cache dedupe:

~~~python
async def test_text_request_uses_text_lane_and_releases_lease(self):
    result = await invoke_chat(model="pro-chat", messages=[{"role": "user", "content": "hi"}])
    self.assertEqual(result.status_code, 200)
    self.assertEqual(fake_scheduler.calls[0].lane, Lane.TEXT)
    self.assertEqual(fake_scheduler.pool.released_count, 1)

async def test_cached_duplicate_does_not_start_second_upstream_call(self):
    await invoke_chat(body)
    await invoke_chat(body)
    self.assertEqual(fake_upstream.call_count, 1)
    self.assertEqual(fake_scheduler.metrics_snapshot()["text"]["admitted"], 1)

def test_partial_stream_failure_is_not_replayed(self):
    list(stream_text_deltas(fake_backend, request))
    self.assertEqual(fake_selector.alternate_attempts, 0)
~~~

- [ ] Step 2: Run focused tests and verify legacy selection remains.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_text_integration.py test/test_text_model_routing.py test/test_v1_chat_completions.py test/test_v1_responses.py test/test_v1_messages.py -q
~~~

Expected: new lane assertions fail because handlers still select through AccountService and LoggedCall.run still uses the default thread pool.

- [ ] Step 3: Add a scheduled execution seam to LoggedCall.

Extend LoggedCall.run while keeping unscheduled compatibility:

~~~python
async def run(self, handler, *args, sse="openai", scheduler_runner=None):
    if scheduler_runner is None:
        result = await run_in_threadpool(handler, *args)
    else:
        result = await scheduler_runner(handler, *args)
    # Preserve the existing dict and stream response/logging branches.
~~~

The scheduled runner returns a dictionary or a stream adapter with the existing terminal error behavior. Keep LoggedCall.stream as the single terminal call-log path.

- [ ] Step 4: Move text acquisition into scheduler execution context.

Add a context-local execution lease to SchedulerService. Update conversation.text_backend and stream_text_deltas to use the active scheduler lease. Before the first delta, invalid-token errors release the failed lease and request an excluded alternate lease within the same deadline. After any delta, record failure and re-raise without replay.

Keep a narrow fallback for direct conversation unit tests that do not install a scheduler context. Production routes must use the scheduler. Call mark_text_used only after successful completion.

- [ ] Step 5: Route all text protocols through the text lane.

In api/ai.py, pass a scheduler runner for chat completions, responses, messages, and search. Keep cache lookup outside the upstream work callback so cache hits do not consume a long-lived lease. Use Lane.TEXT, cost 1, requested model, and the configured text deadline. Preserve anonymous-model behavior.

- [ ] Step 6: Run text tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_text_integration.py test/test_text_model_routing.py test/test_v1_chat_completions.py test/test_v1_responses.py test/test_v1_messages.py -q
git add services/log_service.py services/protocol/conversation.py services/protocol/openai_v1_chat_complete.py services/protocol/openai_v1_response.py services/protocol/anthropic_v1_messages.py api/ai.py test/test_scheduler_text_integration.py test/test_text_model_routing.py test/test_v1_chat_completions.py test/test_v1_responses.py test/test_v1_messages.py
git commit -m "feat: schedule text requests through account leases"
~~~

Expected: text routing, cache, stream retry, cancellation, logging, and lane-isolation tests pass.

## Task 8: Schedule Image Generation and Editing as Work Units

Files:

- Modify: services/protocol/conversation.py
- Modify: services/protocol/openai_v1_image_generations.py
- Modify: services/protocol/openai_v1_image_edit.py
- Modify: services/log_service.py
- Modify: api/ai.py
- Create: test/test_scheduler_image_integration.py
- Modify: image generation, edit, multi-image, response, and chat protocol tests

- [ ] Step 1: Add failing image scheduling tests.

~~~python
async def test_generation_n_four_consumes_four_units(self):
    response_task = asyncio.create_task(invoke_generation(n=4))
    await fake_scheduler.wait_until_active(Lane.IMAGE_GENERATION, cost=4)
    self.assertEqual(fake_scheduler.active_cost(Lane.IMAGE_GENERATION), 4)
    release_outputs()
    self.assertEqual((await response_task).status_code, 200)

async def test_edit_uses_independent_lane_from_generation(self):
    generation = asyncio.create_task(invoke_generation(n=1))
    await fake_scheduler.wait_until_active(Lane.IMAGE_GENERATION)
    edit_response = await invoke_edit()
    self.assertNotEqual(edit_response.status_code, 429)
    release_outputs()
    await generation

async def test_image_queue_full_returns_429_without_thread_growth(self):
    fill_image_lane()
    started_threads = threading.active_count()
    response = await invoke_generation(n=1)
    self.assertEqual(response.status_code, 429)
    self.assertLessEqual(threading.active_count(), started_threads + settings.image_generation.worker_count)
~~~

Add a regression test that a known remote conversation/task ID continues polling instead of submitting a duplicate after a polling timeout.

- [ ] Step 2: Run focused image tests and verify the old executor remains.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_image_integration.py test/test_multi_image_results.py test/test_v1_images_generations.py test/test_v1_images_edits.py test/test_v1_images_edits_api.py -q
~~~

Expected: new lane and no-duplicate assertions fail while stream_image_outputs_with_pool still creates a per-request ThreadPoolExecutor.

- [ ] Step 3: Split image request construction from work-unit formatting.

In both image protocol modules add:

~~~python
def build_request(body: dict[str, Any]) -> ConversationRequest: ...
def format_outputs(outputs: Iterable[ImageOutput], body: dict[str, Any]) -> dict[str, Any] | Iterator[dict[str, Any]]: ...
~~~

Keep mask compositing, image encoding, response usage, and stream chunk formats unchanged. Carry a stable request idempotency key when supplied.

- [ ] Step 4: Replace the local image executor with scheduler work units.

Refactor stream_image_outputs_with_pool to submit one LaneJob per output unit through SchedulerService.run_image. Remove the local ThreadPoolExecutor and as_completed. Preserve output index ordering and partial-success behavior.

Each unit acquires one image-generation or image-edit lease. On success record the scheduler outcome and call mark_image_result(success=True) once. On failure call mark_image_result(success=False) once and release through the scheduler, never through both old and new mechanisms. Remove the old slot release from mark_image_result after all image callers use the new lease, and keep token replacement protected by the AccountService lock.

Implement bounded retry classification: invalid token refresh once; rate limit cools the account; network and 5xx errors may use an alternate account within the deadline; known remote task IDs continue polling; input and content errors do not retry. Release the failed lease before acquiring a replacement.

- [ ] Step 5: Wire direct, chat-image, and response-image routes.

Use:

~~~text
generation -> Lane.IMAGE_GENERATION, cost=payload["n"]
edit       -> Lane.IMAGE_EDIT, cost=payload["n"]
chat image -> the image lane selected by the parsed operation, cost=parsed_n
response image -> the same selected lane, cost=parsed_n
~~~

Validate before admission. Map QueueFullError to 429, NoProviderAvailableError to 503, and image-domain errors to the existing OpenAI image error format.

- [ ] Step 6: Run image tests and commit.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_image_integration.py test/test_multi_image_results.py test/test_v1_images_generations.py test/test_v1_images_edits.py test/test_v1_images_edits_api.py test/test_v1_responses.py test/test_v1_chat_completions.py -q
git add services/protocol/conversation.py services/protocol/openai_v1_image_generations.py services/protocol/openai_v1_image_edit.py services/log_service.py api/ai.py test/test_scheduler_image_integration.py test/test_multi_image_results.py test/test_v1_images_generations.py test/test_v1_images_edits.py test/test_v1_images_edits_api.py test/test_v1_responses.py test/test_v1_chat_completions.py
git commit -m "feat: schedule image work units with bounded lanes"
~~~

Expected: image generation/edit, multi-output, chat-image, response-image, retry, partial-success, and lane-isolation tests pass without the old per-request executor.

## Task 9: Move Asynchronous Image Tasks onto the Bounded Scheduler

Files:

- Modify: services/image_task_service.py
- Modify: api/image_tasks.py
- Modify: services/scheduler/state_store.py
- Create: test/test_scheduler_async_tasks.py
- Modify: test/test_image_task_service.py and test/test_image_tasks_api.py

- [ ] Step 1: Add failing async-task queue tests.

~~~python
def test_submit_does_not_create_one_thread_per_task(self):
    before = {thread.ident for thread in threading.enumerate()}
    task = service.submit_generation(
        OWNER,
        client_task_id="queued",
        prompt="cat",
        model="gpt-image-2",
        size=None,
    )
    self.assertEqual(task["status"], "queued")
    after = {thread.ident for thread in threading.enumerate()}
    self.assertEqual(after - before, set())

def test_scheduler_rejection_is_exposed_as_429(self):
    response = client.post("/api/image-tasks/generations", json=payload, headers=auth)
    self.assertEqual(response.status_code, 429)
~~~

Keep ownership, duplicate-submit, terminal status, cleanup, and resume-poll tests.

- [ ] Step 2: Run focused tests and verify thread-per-task behavior remains.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_async_tasks.py test/test_image_task_service.py test/test_image_tasks_api.py -q
~~~

Expected: no-new-thread and 429 assertions fail against the current daemon-thread implementation.

- [ ] Step 3: Replace task startup threads with scheduler submission.

Inject SchedulerService into ImageTaskService. Persist task metadata, then call scheduler.submit_background with the correct lane and a work callback. The callback updates queued, running, progress, terminal result, and failure state through the existing service methods.

Persist owner, task ID, mode, model, status, remote task/conversation ID, result metadata, error, and timestamps in scheduler_tasks. Do not persist raw base64 input images. Import existing data/image_tasks.json metadata once, preserving terminal IDs and renaming the source only after successful import. Keep current-process result data behavior unchanged.

- [ ] Step 4: Make resume-poll use the same bounded lane.

Replace _run_resume_poll's daemon thread with an image-lane work item. It uses the stored conversation ID, acquires the capability-appropriate lease, and releases it in every outcome. A full queue returns 429 rather than creating an unbounded waiter.

- [ ] Step 5: Map scheduler errors and run tests.

In api/image_tasks.py catch QueueFullError, QueueTimeoutError, and NoProviderAvailableError, returning status and Retry-After. Preserve 400 for validation errors.

~~~bash
.venv/bin/python -m pytest test/test_scheduler_async_tasks.py test/test_image_task_service.py test/test_image_tasks_api.py -q
git add services/image_task_service.py api/image_tasks.py services/scheduler/state_store.py test/test_scheduler_async_tasks.py test/test_image_task_service.py test/test_image_tasks_api.py
git commit -m "feat: bound asynchronous image task execution"
~~~

Expected: task persistence, ownership, idempotency, queue limits, recovery, and API status tests pass.

## Task 10: Remove Legacy Selectors, Add Metrics, and Verify Scale

Files:

- Modify: services/account_service.py
- Modify: services/scheduler/service.py
- Modify: services/scheduler/account_pool.py
- Modify: services/scheduler/lane.py
- Modify: services/protocol/conversation.py
- Create: test/test_scheduler_integration.py
- Create: scripts/benchmark_scheduler.py
- Modify: account and routing regression tests

- [ ] Step 1: Add integration invariants.

~~~python
@pytest.mark.parametrize("account_count", [1, 20, 200, 2000])
def test_selection_has_no_full_scan(account_count):
    scheduler = make_scheduler(make_accounts(account_count))
    run_requests(scheduler, count=100, lane=Lane.TEXT)
    assert scheduler.account_pool.selection_probe_count < 500

def test_image_lane_cannot_exhaust_reserved_text_capacity(self):
    fill_lane(self.scheduler, Lane.IMAGE_GENERATION)
    lease = self.scheduler.account_pool.try_acquire(text_requirement, deadline=self.clock.now + 5)
    self.assertIsNotNone(lease)

def test_expired_lease_is_reclaimed_without_negative_counters(self):
    lease = self.scheduler.account_pool.try_acquire(text_requirement, deadline=self.clock.now + 1)
    self.clock.advance(2)
    self.scheduler.reclaim_expired()
    self.assertEqual(self.scheduler.account_pool.inflight(lease.account_key, Lane.TEXT), 0)

def test_breaker_isolated_by_account_and_lane(self):
    self.scheduler.account_pool.open_breaker("shared", Lane.IMAGE_GENERATION)
    self.assertIsNotNone(self.scheduler.account_pool.try_acquire(text_requirement, deadline=self.clock.now + 5))
    self.assertIsNone(self.scheduler.account_pool.try_acquire(image_requirement, deadline=self.clock.now + 5))

def test_overload_returns_429_before_gateway_timeout(self):
    fill_lane(self.scheduler, Lane.TEXT)
    with self.assertRaises(QueueFullError):
        self.run_text_request("hello")

def test_account_add_remove_reconciles_after_generation_change(self):
    generation = self.account_service.scheduler_state_generation()
    self.account_service.add_account_items([make_account("new")])
    self.scheduler.reconcile_if_changed()
    self.assertGreater(self.account_service.scheduler_state_generation(), generation)
    self.assertIn("new", self.scheduler.account_pool.account_keys())
~~~

Use fake upstream work with controlled latency. Assert no account exceeds its lane or total limits, all leases release, and request selection never writes SQLite.

- [ ] Step 2: Remove legacy hot-path scheduling.

After every production route uses SchedulerService, remove _index, _image_inflight, _image_slot_condition, _acquire_next_candidate_token, and candidate-list scans from AccountService. Keep persistence, token refresh, mark_text_used, mark_image_result, and public reporting. Update old tests to assert scheduler behavior.

If a public compatibility method must remain, make it a thin scheduler adapter that never waits on a condition or scans all accounts per request.

- [ ] Step 3: Add metrics and an offline benchmark.

Expose lane and account metrics through metrics_snapshot and the protected system API. Add scripts/benchmark_scheduler.py with --accounts, --requests, and --lane. It uses fake fixed-latency work and prints account count, admitted/rejected, throughput, p50/p95 queue wait, p95 service time, max in-flight by lane, and lease leaks. It never calls an upstream service.

- [ ] Step 4: Run the complete Python suite.

~~~bash
.venv/bin/python -m pytest test -q
~~~

Expected: all existing and new tests pass with zero failures. If .venv/bin/python is unavailable, run the same command with the configured project interpreter and record the substitute.

- [ ] Step 5: Run offline scale benchmarks.

~~~bash
.venv/bin/python scripts/benchmark_scheduler.py --accounts 20 --requests 200 --lane text
.venv/bin/python scripts/benchmark_scheduler.py --accounts 200 --requests 200 --lane image_generation
.venv/bin/python scripts/benchmark_scheduler.py --accounts 2000 --requests 2000 --lane text
~~~

Expected: selection probes remain sublinear per request, no lease leaks occur, lane capacities are never exceeded, and overload is reported as explicit rejection rather than unbounded queue growth.

- [ ] Step 6: Commit cleanup and verification harness.

~~~bash
git add services/account_service.py services/scheduler services/protocol/conversation.py test/test_scheduler_integration.py scripts/benchmark_scheduler.py
git commit -m "test: verify scheduler invariants and scale"
~~~

## Final Verification Checklist

- [ ] git status --short is clean.
- [ ] python -m pytest test -q passes.
- [ ] Hot-path account selection does not scan all accounts or synchronously write SQLite.
- [ ] Text, image generation, and image editing have independent bounded lanes.
- [ ] n=4 consumes four image work units.
- [ ] Waiting requests do not create one thread each.
- [ ] Leases release on success, error, timeout, cancellation, and watchdog recovery.
- [ ] Text retries stop after partial output.
- [ ] Image polling never blindly resubmits a known remote task.
- [ ] Scheduler state writes are coalesced and separate from data/logs.db.
- [ ] Overload returns 429/503 with Retry-After instead of indefinite waiting.
- [ ] Offline benchmarks cover 20, 200, and 2000 accounts.
