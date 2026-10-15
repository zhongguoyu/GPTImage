# User Key Quota System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为普通用户密钥增加永久文本额度和共享图片额度，并在同步接口、流式接口和持久化图片任务中以 SQLite 原子预留/结算/释放的方式执行限制，管理员密钥保持无限额度。

**Architecture:** 新增独立的 `AuthQuotaStore`/`QuotaService`，数据库固定为 `data/auth_quota.db`，使用 WAL、`BEGIN IMMEDIATE` 和幂等 reservation 状态迁移来防止并发超扣。API 层通过统一额度包装器把请求生命周期交给 `LoggedCall` 的成功、失败和流式观察回调；图片任务把 reservation id 写入现有 scheduler SQLite 任务元数据，恢复时继续结算原 reservation。管理 API 返回 limit/used/reserved/remaining，Web 端编辑总额度并每 5 秒轮询可见页面。

**Tech Stack:** Python 3.13, FastAPI, Pydantic, stdlib `sqlite3`, existing scheduler SQLite state store, Next.js/React/TypeScript, pytest/unittest.

---

## 文件结构

- Create: `services/quota_service.py`，额度 SQLite schema、事务、reservation 状态机、额度快照、watchdog 和流式结果计数。
- Create: `test/test_quota_service.py`，额度服务单测、SQLite 并发测试和过期恢复测试。
- Create: `test/test_quota_api.py`，用户密钥额度管理 API 测试。
- Modify: `services/log_service.py`，为 `LoggedCall` 增加成功/失败/流式 item 回调，并把取消和生成器关闭视为失败结算路径。
- Modify: `test/test_log_service.py`，验证回调只在正确的生命周期阶段触发。
- Modify: `api/support.py`，加入统一额度执行包装器和 `429 insufficient_quota` 映射。
- Modify: `api/accounts.py`，创建、列表、修改、删除用户密钥时同步管理额度。
- Modify: `services/auth_service.py`，保留现有认证字段边界，不把额度写入 auth key JSON；必要时只暴露创建/更新后的公开用户密钥对象。
- Modify: `api/ai.py`，文本、直接图片及混合 chat/responses 请求接入额度包装器。
- Modify: `services/scheduler/state_store.py`，允许安全持久化 image task 的 quota reservation 元数据。
- Modify: `services/image_task_service.py`，创建任务时预留，成功按实际图片数提交，失败释放，超时可恢复任务复用原 reservation。
- Modify: `api/app.py`，启动时初始化并迁移额度行、启动 watchdog、退出时停止 watchdog。
- Modify: `services/backup_service.py`，把 SQLite 额度库以一致快照加入 auth key 备份。
- Modify: `test/test_image_task_service.py`，覆盖任务额度生命周期和恢复不重复扣减。
- Modify: `test/test_image_tasks_api.py`，保持现有 fake service 兼容并验证额度不足由服务层返回。
- Modify: `web/src/lib/api.ts`，扩展额度类型和管理 API 参数。
- Modify: `web/src/app/settings/components/user-keys-card.tsx`，额度创建/编辑/展示和可见页面轮询。

### Task 1: SQLite quota store and service

**Files:**
- Create: `test/test_quota_service.py`
- Create: `services/quota_service.py`

- [ ] **Step 1: Write the failing service tests**

Add tests using `QuotaService(AuthQuotaStore(tmp_path / "auth_quota.db"))` and identities such as `{"id": "user-1", "role": "user"}` and `{"id": "admin", "role": "admin"}`. The first test set must assert these exact behaviors:

```python
def test_new_user_has_zero_quota_and_admin_bypasses(tmp_path):
    service = make_service(tmp_path)

    self.assertEqual(service.get_quota("user-1")["text_remaining"], 0)
    with self.assertRaises(QuotaExceededError):
        service.reserve(user_identity("user-1"), "text", 1, request_id="r-1", endpoint="/v1/chat/completions")

    reservation = service.reserve(admin_identity(), "image", 4, request_id="admin-1", endpoint="/v1/images/generations")
    self.assertTrue(reservation.admin_bypass)
    self.assertIsNone(service.get_quota("admin"))

def test_reserve_commit_and_release_keep_remaining_consistent(tmp_path):
    service = make_service(tmp_path)
    service.set_limits("user-1", text_limit=2, image_limit=4)

    text = service.reserve(user_identity("user-1"), "text", 1, request_id="t-1", endpoint="/v1/chat/completions")
    image = service.reserve(user_identity("user-1"), "image", 4, request_id="i-1", endpoint="/v1/images/generations")
    quota = service.get_quota("user-1")
    self.assertEqual((quota["text_used"], quota["text_reserved"], quota["text_remaining"]), (0, 1, 1))
    self.assertEqual((quota["image_used"], quota["image_reserved"], quota["image_remaining"]), (0, 4, 0))

    self.assertTrue(service.commit(text.reservation_id, 1))
    self.assertTrue(service.commit(image.reservation_id, 3))
    self.assertTrue(service.release(image.reservation_id))
    self.assertFalse(service.commit(image.reservation_id, 1))
    quota = service.get_quota("user-1")
    self.assertEqual((quota["text_used"], quota["text_reserved"], quota["text_remaining"]), (1, 0, 1))
    self.assertEqual((quota["image_used"], quota["image_reserved"], quota["image_remaining"]), (3, 0, 1))

def test_limit_update_cannot_drop_below_used_or_reserved(tmp_path):
    service = make_service(tmp_path)
    service.set_limits("user-1", text_limit=3, image_limit=3)
    reservation = service.reserve(user_identity("user-1"), "text", 2, request_id="r-1", endpoint="/v1/chat/completions")
    service.commit(reservation.reservation_id, 1)
    with self.assertRaises(QuotaLimitError):
        service.set_limits("user-1", text_limit=0, image_limit=3)
    with self.assertRaises(QuotaLimitError):
        service.set_limits("user-1", text_limit=1, image_limit=3)

def test_expired_reservation_is_released_once(tmp_path):
    service = make_service(tmp_path)
    service.set_limits("user-1", text_limit=1, image_limit=1)
    reservation = service.reserve(
        user_identity("user-1"), "text", 1, request_id="r-1", endpoint="/v1/chat/completions", expires_at=time.time() - 1
    )
    self.assertEqual(service.recover_expired(now=time.time()), 1)
    self.assertEqual(service.recover_expired(now=time.time()), 0)
    self.assertEqual(service.get_quota("user-1")["text_reserved"], 0)
    self.assertEqual(service.get_reservation(reservation.reservation_id)["status"], "expired")

def test_concurrent_reservations_never_exceed_limit(tmp_path):
    service = make_service(tmp_path)
    service.set_limits("user-1", text_limit=8, image_limit=0)
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda index: reserve_text(service, index), range(16)))
    self.assertEqual(sum(result is not None for result in results), 8)
    self.assertLessEqual(service.get_quota("user-1")["text_reserved"], 8)
```

The test helpers must create a fresh `AuthQuotaStore` and call `initialize()` so no repository `data` database is touched.

- [ ] **Step 2: Run the focused tests and verify the expected red failure**

Run:

```bash
uv run pytest -q test/test_quota_service.py
```

Expected: collection or test failures because `services.quota_service` and its public classes do not exist yet.

- [ ] **Step 3: Implement the minimal SQLite store and service**

Implement `AuthQuotaStore` with a thread-local connection configured on every connection:

```python
connection = sqlite3.connect(str(self.db_path), timeout=5.0)
connection.row_factory = sqlite3.Row
connection.execute("PRAGMA journal_mode=WAL")
connection.execute("PRAGMA synchronous=NORMAL")
connection.execute("PRAGMA busy_timeout=5000")
```

Create `key_quotas` and `quota_reservations` exactly as specified in `docs/superpowers/specs/2026-09-08-user-key-quota-design.md`, plus indexes on `quota_reservations(key_id, status)` and `quota_reservations(expires_at, status)`. Use explicit `BEGIN IMMEDIATE` for `reserve`, `commit`, `release`, `set_limits`, and `recover_expired`; calculate remaining as `limit - used - reserved`, never persist it.

Expose these public types and methods:

```python
QuotaType = Literal["text", "image"]

class QuotaExceededError(Exception): ...
class QuotaLimitError(Exception): ...

@dataclass(frozen=True)
class QuotaReservation:
    reservation_id: str
    key_id: str | None
    quota_type: QuotaType
    requested_units: int
    admin_bypass: bool = False

class QuotaService:
    def initialize(self) -> None: ...
    def ensure_user_keys(self, items: Iterable[dict[str, object]]) -> None: ...
    def ensure_key(self, key_id: str) -> None: ...
    def mark_deleted(self, key_id: str) -> None: ...
    def reserve(self, identity: dict[str, object], quota_type: QuotaType, units: int, *, request_id: str = "", endpoint: str = "", expires_at: float | None = None) -> QuotaReservation: ...
    def commit(self, reservation_id: str, committed_units: int) -> bool: ...
    def release(self, reservation_id: str) -> bool: ...
    def recover_expired(self, now: float | None = None) -> int: ...
    def get_quota(self, key_id: str) -> dict[str, int | str | None] | None: ...
    def list_user_quotas(self, items: list[dict[str, object]]) -> list[dict[str, object]]: ...
    def set_limits(self, key_id: str, *, text_limit: int | None = None, image_limit: int | None = None) -> dict[str, object]: ...
    def get_reservation(self, reservation_id: str) -> dict[str, object] | None: ...
    def list_reservations(self, key_id: str) -> list[dict[str, object]]: ...
    def start_watchdog(self, stop_event: threading.Event, interval_seconds: float = 5.0) -> threading.Thread: ...
```

`reserve` must treat `role == "admin"` as an in-memory bypass with no row. A missing user row must be lazily inserted with zero limits. `commit`/`release` must return `False` for an already terminal reservation and must never mutate the summary twice. `commit` must reject values outside `0..requested_units`. A reservation that has reached `expired` must not be committed later.

Use a fixed default TTL of 900 seconds when callers omit `expires_at`; image tasks may pass a longer TTL derived from `config.image_poll_timeout_secs`. Add `snapshot_bytes()` using the SQLite backup API for consistent backup integration later.

- [ ] **Step 4: Run the focused tests and verify green**

Run:

```bash
uv run pytest -q test/test_quota_service.py
```

Expected: all quota store/service tests pass, including the 16-thread reservation test.

- [ ] **Step 5: Commit the quota service**

```bash
git add services/quota_service.py test/test_quota_service.py
git commit -m "feat: add sqlite user key quota service"
```

### Task 2: Quota accounting around LoggedCall

**Files:**
- Modify: `services/log_service.py`
- Modify: `api/support.py`
- Modify: `test/test_log_service.py`
- Modify: `test/test_quota_service.py`

- [ ] **Step 1: Write failing lifecycle tests**

Add tests that pass callback functions to `LoggedCall.run` and consume returned streams. They must prove a dict response invokes `on_success` once, a handled `HTTPException` invokes `on_failure` once, a normal async stream invokes `on_success` once, and a cancelled/closed async stream invokes `on_failure` without success. Add result counter tests:

```python
def test_count_image_outputs_handles_direct_response_partial_results():
    self.assertEqual(count_image_outputs({"data": [{"b64_json": "a"}, {"url": "https://x/2"}]}), 2)
    self.assertEqual(count_image_outputs({"data": [{"b64_json": "a"}]}), 1)

def test_count_image_outputs_handles_responses_and_chat_shapes():
    self.assertEqual(count_image_outputs({"response": {"output": [{"type": "image_generation_call"}]} }), 1)
    self.assertEqual(count_image_outputs({"choices": [{"message": {"content": "![image](data:image/png;base64,abc)"}}]}), 1)

def test_stream_accounting_counts_partial_image_chunks():
    accounting = QuotaAccounting(
        QuotaReservation(
            reservation_id="reservation-1",
            key_id="user-1",
            quota_type="image",
            requested_units=4,
        ),
        "image",
    )
    accounting.observe({"object": "image.generation.result", "data": [{"b64_json": "a"}]})
    accounting.observe({"object": "image.generation.result", "data": [{"b64_json": "b"}, {"b64_json": "c"}]})
    self.assertEqual(accounting.success_units(), 3)
```

- [ ] **Step 2: Run the lifecycle tests and verify red**

Run:

```bash
uv run pytest -q test/test_log_service.py test/test_quota_service.py
```

Expected: failures for the missing callback parameters and accounting helpers.

- [ ] **Step 3: Implement lifecycle callbacks and the quota execution wrapper**

Extend `LoggedCall.run` with optional callbacks:

```python
async def run(
    self,
    handler,
    *args,
    sse: str = "openai",
    scheduler_runner=None,
    streaming: bool = False,
    on_success: Callable[[object | None], None] | None = None,
    on_failure: Callable[[], None] | None = None,
    on_stream_item: Callable[[object], None] | None = None,
):
```

Pass `on_stream_item`, `on_success`, and `on_failure` into `stream()` and `async_stream()`. Call failure callbacks for all handled exceptions and for `BaseException` raised while a stream is consumed. Call success only after the whole stream finishes normally; an empty text stream is successful, while an empty image stream is released by the accounting object. Keep callback exceptions isolated from request handling. Preserve existing logging and response formatting.

Add `QuotaAccounting` and `run_with_quota` to `api/support.py`:

```python
async def run_with_quota(
    call: LoggedCall,
    handler,
    *args,
    identity: dict[str, object],
    quota_type: QuotaType,
    requested_units: int,
    endpoint: str,
    sse: str = "openai",
    scheduler_runner=None,
    streaming: bool = False,
):
    reservation = quota_service.reserve(identity, quota_type, requested_units, request_id=uuid.uuid4().hex, endpoint=endpoint)
    accounting = QuotaAccounting(reservation, quota_type)
    try:
        return await call.run(
            handler,
            *args,
            sse=sse,
            scheduler_runner=scheduler_runner,
            streaming=streaming,
            on_success=accounting.commit_success,
            on_failure=accounting.release,
            on_stream_item=accounting.observe,
        )
    except BaseException:
        accounting.release()
        raise
```

Map `QuotaExceededError` to `HTTPException(429, detail={"error": {"message": "text quota exhausted" or "image quota exhausted", "type": "insufficient_quota", "code": "insufficient_quota"}})`. `QuotaAccounting` must commit text as 1 and image as `min(requested_units, observed_or_result_units)`; zero observed image outputs release the reservation. Its `observe` method must count direct image result chunks, Responses `response.completed` image output items, and `data:image/` occurrences in chat image chunks. All settlement methods are idempotent.

- [ ] **Step 4: Run the lifecycle tests and existing logging tests**

Run:

```bash
uv run pytest -q test/test_log_service.py test/test_quota_service.py
```

Expected: all tests pass and existing log behavior remains unchanged.

- [ ] **Step 5: Commit the accounting layer**

```bash
git add services/log_service.py api/support.py test/test_log_service.py test/test_quota_service.py
git commit -m "feat: add request quota accounting lifecycle"
```

### Task 3: User-key quota management API and migration

**Files:**
- Modify: `api/accounts.py`
- Modify: `api/app.py`
- Modify: `services/auth_service.py`
- Create: `test/test_quota_api.py`

- [ ] **Step 1: Write failing API tests**

Use a FastAPI test app with patched `require_admin`, `auth_service`, and a temporary `QuotaService`. Cover:

```python
def test_create_user_key_accepts_initial_limits(self):
    response = self.client.post("/api/auth/users", json={"name": "设计组", "text_limit": 10, "image_limit": 4})
    self.assertEqual(response.status_code, 200, response.text)
    self.assertEqual(response.json()["item"]["text_limit"], 10)
    self.assertEqual(response.json()["item"]["image_limit"], 4)

def test_list_and_update_return_remaining_and_reject_low_limit(self):
    listed = self.client.get("/api/auth/users")
    self.assertEqual(listed.json()["items"][0]["text_remaining"], 9)
    response = self.client.post("/api/auth/users/user-1", json={"text_limit": 0})
    self.assertEqual(response.status_code, 409)
    self.assertEqual(response.json()["detail"]["code"], "quota_limit_below_usage")

def test_delete_marks_quota_history_deleted_after_auth_key_delete(self):
    response = self.client.delete("/api/auth/users/user-1")
    self.assertEqual(response.status_code, 200)
    self.quota_service.store.get_quota("user-1")
    self.assertIsNotNone(self.quota_service.get_quota("user-1"))
```

- [ ] **Step 2: Run the API tests and verify red**

Run:

```bash
uv run pytest -q test/test_quota_api.py
```

Expected: request model and quota fields are missing, and no quota merge occurs.

- [ ] **Step 3: Implement management and startup migration**

Add `text_limit: int = Field(default=0, ge=0)` and `image_limit: int = Field(default=0, ge=0)` to create/update bodies. The API must:

1. Create the auth key through `auth_service`, then ensure its quota row and set both initial limits.
2. List `auth_service.list_keys(role="user")` merged with `quota_service.list_user_quotas(...)`.
3. Treat quota-only updates as valid; pass auth fields to `auth_service.update_key` and quota fields to `quota_service.set_limits`.
4. Map `QuotaLimitError` to `409` with code `quota_limit_below_usage`, and do not partially update the two quota limits.
5. On delete, delete the auth key and call `quota_service.mark_deleted(key_id)` without deleting the quota row.

Call `quota_service.initialize()` and `quota_service.ensure_user_keys(auth_service.list_keys(role="user"))` during app lifespan startup before serving requests. The migration must insert missing user rows with zero values and never overwrite existing limits, used, reserved, or deleted rows. Start `quota_service.start_watchdog(stop_event)` and join it during shutdown.

- [ ] **Step 4: Run API, config, storage, and scheduler regression tests**

Run:

```bash
uv run pytest -q test/test_quota_api.py test/test_config.py test/test_database_storage.py test/test_scheduler_config.py
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit the management API**

```bash
git add api/accounts.py api/app.py services/auth_service.py test/test_quota_api.py
git commit -m "feat: expose user key quota management API"
```

### Task 4: Text and direct image API charging

**Files:**
- Modify: `api/ai.py`
- Create: `test/test_quota_request_integration.py`

- [ ] **Step 1: Write failing endpoint integration tests**

Patch `ai_module.require_identity`, `ai_module.filter_or_log`, the protocol handlers, `ai_module.quota_service`, and the scheduler runners. Assert the reserve/commit/release calls and HTTP status codes:

```python
def test_text_success_commits_one(self):
    response = self.client.post("/v1/chat/completions", json={"model": "auto", "messages": [{"role": "user", "content": "hi"}]})
    self.assertEqual(response.status_code, 200)
    self.assertEqual(self.quota.commit_units, [1])

def test_text_handler_failure_releases_without_commit(self):
    response = self.client.post("/v1/search", json={"prompt": "hi"})
    self.assertEqual(response.status_code, 502)
    self.assertEqual(self.quota.commit_units, [])
    self.assertEqual(self.quota.release_count, 1)

def test_image_partial_response_commits_returned_count(self):
    response = self.client.post("/v1/images/generations", json={"prompt": "four", "n": 4})
    self.assertEqual(response.status_code, 200)
    self.assertEqual(self.quota.reserved_units, [4])
    self.assertEqual(self.quota.commit_units, [3])

def test_quota_exhaustion_returns_429_without_handler_call(self):
    response = self.client.post("/v1/images/generations", json={"prompt": "one", "n": 1})
    self.assertEqual(response.status_code, 429)
    self.assertEqual(self.handler_calls, 0)
```

Include text stream success/failure and image stream partial-output tests by consuming `response.body_iterator` to completion or raising cancellation.

- [ ] **Step 2: Run the integration tests and verify red**

Run:

```bash
uv run pytest -q test/test_quota_request_integration.py
```

Expected: endpoints do not yet invoke quota reserve/settle.

- [ ] **Step 3: Route all counted AI requests through `run_with_quota`**

In `api/ai.py`, keep existing validation and `filter_or_log` before reserving. Use:

```python
return await run_with_quota(
    call,
    handler,
    payload,
    identity=identity,
    quota_type="text" if not is_image_request else "image",
    requested_units=1 if not is_image_request else requested_image_count,
    endpoint="/v1/chat/completions",
    scheduler_runner=scheduler_runner,
    streaming=bool(payload.get("stream")),
)
```

Apply the same pattern to `/v1/responses`, `/v1/messages`, `/v1/search`, `/v1/images/generations`, and `/v1/images/edits`. Mixed chat/responses requests that select an image lane use image quota; normal text and search use one text unit. Derive image request counts from the already validated `n` or image-generation tool and cap at `MAX_IMAGE_OUTPUTS`. Do not add quota calls to models, file tasks, task queries, management endpoints, or health checks.

- [ ] **Step 4: Run the integration and protocol regression tests**

Run:

```bash
uv run pytest -q test/test_quota_request_integration.py test/test_v1_images_edits_json.py test/test_v1_images_generations.py test/test_v1_responses.py test/test_v1_messages.py test/test_gpt_search.py
```

Expected: all mocked integration tests pass; live upstream tests may be skipped or unavailable and must be reported separately rather than treated as local quota failures.

- [ ] **Step 5: Commit API charging**

```bash
git add api/ai.py test/test_quota_request_integration.py
git commit -m "feat: charge user key quotas on ai requests"
```

### Task 5: Persistent image-task quota lifecycle

**Files:**
- Modify: `services/scheduler/state_store.py`
- Modify: `services/image_task_service.py`
- Modify: `api/image_tasks.py`
- Modify: `test/test_image_task_service.py`
- Modify: `test/test_image_tasks_api.py`

- [ ] **Step 1: Write failing task lifecycle tests**

Inject a temporary `QuotaService` into `ImageTaskService` and assert:

```python
def test_new_task_reserves_one_image_and_success_commits_one(self):
    service = make_service(quota_service=self.quota)
    service.submit_generation(OWNER_USER, client_task_id="task-1", prompt="cat", model="gpt-image-2", size=None)
    self.assertEqual(self.quota.get_quota("user-1")["image_reserved"], 1)
    wait_for_task(service, OWNER_USER, "task-1", "success")
    self.assertEqual(self.quota.get_quota("user-1")["image_used"], 1)
    self.assertEqual(self.quota.get_quota("user-1")["image_reserved"], 0)

def test_duplicate_client_task_id_does_not_reserve_twice(self):
    service = make_service(quota_service=self.quota)
    service.submit_generation(OWNER_USER, client_task_id="same", prompt="cat", model="gpt-image-2", size=None)
    service.submit_generation(OWNER_USER, client_task_id="same", prompt="cat", model="gpt-image-2", size=None)
    self.assertEqual(self.quota.get_quota("user-1")["image_reserved"], 1)

def test_task_failure_releases_and_resume_reuses_reservation(self):
    # first handler raises with a conversation_id and timeout text; resume poll returns two URLs
    reservations = self.quota.list_reservations("user-1")
    self.assertEqual(len(reservations), 1)
    self.assertEqual(self.quota.get_quota("user-1")["image_used"], 2)
    self.assertEqual(self.quota.get_quota("user-1")["image_reserved"], 0)
```

Add an assertion that a scheduler admission failure releases the reservation, and an assertion that admin tasks never create a quota row.

- [ ] **Step 2: Run task tests and verify red**

Run:

```bash
uv run pytest -q test/test_image_task_service.py test/test_image_tasks_api.py
```

Expected: the injected quota service is not accepted and task completion does not change quota state.

- [ ] **Step 3: Persist and settle image task reservations**

Extend `SchedulerStateStore._SAFE_METADATA_KEYS` with `quota_reservation_id` and `quota_requested_units`. `ImageTaskService` must:

1. Check an existing `(owner_id, client_task_id)` while holding its lock before reserving, preserving idempotency.
2. Reserve one image unit for each new generation/edit task after input validation and before saving the task.
3. Persist reservation id and requested units in task metadata; load them on startup.
4. Commit `len(data)` before marking a task successful. If commit is refused because the reservation expired, mark the task failed and do not expose an uncharged success result.
5. Release on ordinary handler failure, cancellation, scheduler admission failure, or unrecoverable resume failure.
6. Keep a reservation for a timeout that has a valid conversation id and can be resumed; `resume_poll` must never reserve again and must commit the original reservation on recovered success.
7. Use a TTL covering `config.image_poll_timeout_secs` plus a 5-minute grace period; renew or extend the reservation before resume when needed.
8. Release reservations during `_submit_async` cleanup if admission fails, with idempotent release protecting duplicate cleanup paths.

Keep task API behavior unchanged and continue returning HTTP 400 for validation and scheduler-specific status codes for queue errors. Quota exhaustion from task creation must surface as the same HTTP 429 `insufficient_quota` response used by synchronous APIs.

- [ ] **Step 4: Run task, scheduler, and quota tests**

Run:

```bash
uv run pytest -q test/test_image_task_service.py test/test_image_tasks_api.py test/test_scheduler_state_store.py test/test_quota_service.py
```

Expected: all task quota lifecycle and existing persistence tests pass.

- [ ] **Step 5: Commit task charging**

```bash
git add services/scheduler/state_store.py services/image_task_service.py api/image_tasks.py test/test_image_task_service.py test/test_image_tasks_api.py
git commit -m "feat: settle quotas for persistent image tasks"
```

### Task 6: Backup and runtime recovery

**Files:**
- Modify: `services/backup_service.py`
- Create: `test/test_backup_quota.py`

- [ ] **Step 1: Write the failing backup test**

Patch `quota_service.snapshot_bytes()` to return a known SQLite payload, build a backup archive with `auth_keys_snapshot` enabled, and assert the tar contains `data/auth_quota.db` alongside `snapshots/auth_keys.json`.

- [ ] **Step 2: Run the backup test and verify red**

```bash
uv run pytest -q test/test_backup_quota.py
```

Expected: the archive does not yet contain `data/auth_quota.db`.

- [ ] **Step 3: Add a consistent quota snapshot to backup**

Import the quota service in `services/backup_service.py`. When `include.auth_keys_snapshot` is true, write the bytes returned by `quota_service.snapshot_bytes()` as `data/auth_quota.db`; do not copy the live WAL main file directly. Keep the existing auth key JSON snapshot unchanged.

- [ ] **Step 4: Run backup and startup regression tests**

```bash
uv run pytest -q test/test_backup_quota.py test/test_scheduler_state_store.py test/test_log_service.py
```

- [ ] **Step 5: Commit runtime recovery and backup**

```bash
git add services/backup_service.py test/test_backup_quota.py
git commit -m "feat: back up user quota database consistently"
```

### Task 7: Web quota management

**Files:**
- Modify: `web/src/lib/api.ts`
- Modify: `web/src/app/settings/components/user-keys-card.tsx`

- [ ] **Step 1: Extend the client types and API functions**

Add these fields to `UserKey`:

```ts
text_limit: number;
text_used: number;
text_reserved: number;
text_remaining: number;
image_limit: number;
image_used: number;
image_reserved: number;
image_remaining: number;
```

Change `createUserKey(name, textLimit, imageLimit)` to post all three values, and change `updateUserKey` to accept `text_limit` and `image_limit` in addition to existing auth fields.

- [ ] **Step 2: Add create/edit controls and live quota display**

Add string state for create/edit quota inputs so empty input can be validated before conversion. Validate non-negative safe integers in the UI, show total/used/reserved/remaining for both quota types on every key row, and include both limits in create and edit requests. Display `409 quota_limit_below_usage` as a clear toast while leaving the dialog open.

Use a visibility-aware polling effect:

```tsx
useEffect(() => {
  const refresh = () => {
    if (!document.hidden) void load(false);
  };
  const interval = window.setInterval(refresh, 5000);
  document.addEventListener("visibilitychange", refresh);
  return () => {
    window.clearInterval(interval);
    document.removeEventListener("visibilitychange", refresh);
  };
}, []);
```

Make `load(showLoading = true)` avoid replacing the list with a spinner during background refresh. Preserve the one-time raw-key reveal and existing enable/disable/delete flows.

- [ ] **Step 3: Run web lint/type checks**

Run:

```bash
cd web && bun run lint && bun run build
```

Expected: no TypeScript, ESLint, or Next.js build errors.

- [ ] **Step 4: Commit Web quota management**

```bash
git add web/src/lib/api.ts web/src/app/settings/components/user-keys-card.tsx
git commit -m "feat: manage user key quotas in web settings"
```

### Task 8: Full verification and inline review

**Files:**
- All implementation files from Tasks 1-7.

- [ ] **Step 1: Run the complete local Python test suite**

```bash
uv run pytest -q
```

Expected: zero failures. Live upstream integration tests that require network credentials must be identified by their existing test behavior and reported separately if unavailable; all local unit/API tests must pass.

- [ ] **Step 2: Run static and repository hygiene checks**

```bash
git diff --check
rg -n -i 'TODO|TBD|FIXME|placeholder|print\(' services/quota_service.py api/support.py api/accounts.py api/ai.py services/image_task_service.py web/src/lib/api.ts web/src/app/settings/components/user-keys-card.tsx
```

Review every match and remove newly introduced placeholders or sensitive debug output. Existing intentional `print` calls outside the changed quota paths must not be changed without a separate reason.

- [ ] **Step 3: Perform inline code review against the approved specification**

Review the final diff in this order:

1. No admin identity creates or consumes a quota row.
2. Missing ordinary-user rows fail closed at zero.
3. Reservation, commit, release, and expiry mutate summary counters exactly once under `BEGIN IMMEDIATE`.
4. Text commits only after normal success; stream cancellation releases.
5. Image commits actual returned outputs, including partial multi-output responses.
6. Image task duplicate submit, scheduler rejection, restart recovery, timeout resume, delete, and late result paths do not double-charge or expose an uncharged success.
7. Quota-only API updates are atomic across the two limit columns and return the documented error codes.
8. Web polling pauses while hidden and does not reset the form or one-time key reveal.
9. Backup captures a consistent quota database snapshot.

Use `git diff 26b30f7..HEAD`, inspect all changed line ranges, and run targeted tests again for every finding fixed. Do not request or start a background review subagent; review remains inline as requested.

- [ ] **Step 4: Verify final repository state before handoff**

```bash
git status --short
git log --oneline -12
```

Expected: only intentional commits from this implementation branch are present and no untracked generated database or build artifacts were added.
