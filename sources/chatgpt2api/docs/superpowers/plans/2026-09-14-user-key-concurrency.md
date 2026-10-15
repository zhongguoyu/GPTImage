# User Key Concurrency Limits Implementation Plan

> For agentic workers: REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Add per-user-key text and image in-flight concurrency limits, configure them from the web settings page, and make saved values effective for new requests immediately.

**Architecture:** A process-local, thread-safe UserKeyConcurrencyLimiter owns idempotent leases keyed by (user_id, request_type). api.support.run_with_quota acquires a lease for synchronous and streaming /v1 work, while ImageTaskService owns leases for asynchronous image tasks until terminal completion. The limiter reads current ConfigStore values on each admission, so changing settings does not require rebuilding the existing scheduler.

**Tech Stack:** Python 3.13, FastAPI, unittest, existing quota/logging/image-task services, Next.js 16, TypeScript, Zustand.

---

## Files And Responsibilities

- Create: services/user_key_concurrency.py - request types, limit exception, thread-safe limiter, and idempotent lease.
- Modify: services/config.py - defaults, positive-integer normalization, accessors, and public settings fields.
- Modify: api/support.py - acquire/release leases around quota-backed /v1 requests and build compatible 429 details.
- Modify: services/image_task_service.py - inject the limiter, retain task leases, release every failure/terminal path, and acquire a fresh lease for resume_poll.
- Modify: api/image_tasks.py - map image-task concurrency failures to 429 with Retry-After.
- Modify: web/src/lib/api.ts, web/src/app/settings/store.ts, web/src/app/settings/components/config-card.tsx - typed fields, store state, and controls.
- Create/modify tests: test/test_user_key_concurrency.py, test/test_config.py, test/test_quota_request_integration.py, test/test_image_task_service.py, and test/test_image_tasks_api.py.

## Task 1: Add And Test Configuration Values

**Files:**
- Modify: test/test_config.py
- Modify: services/config.py

- [ ] Step 1: Write failing configuration tests

Append a temporary-file test class to test/test_config.py:

~~~python
class UserKeyConcurrencyConfigTests(unittest.TestCase):
    def make_store(self, tmp_dir, **values):
        path = Path(tmp_dir) / "config.json"
        path.write_text(json.dumps({"auth-key": "admin", **values}), encoding="utf-8")
        return self.config_module.ConfigStore(path)

    def test_defaults_are_four_and_one(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = self.make_store(tmp_dir)
            self.assertEqual(store.user_key_text_concurrency, 4)
            self.assertEqual(store.user_key_image_concurrency, 1)
            self.assertEqual(store.get()["user_key_text_concurrency"], 4)
            self.assertEqual(store.get()["user_key_image_concurrency"], 1)

    def test_update_normalizes_and_persists_positive_values(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = self.make_store(tmp_dir)
            result = store.update({
                "user_key_text_concurrency": "7",
                "user_key_image_concurrency": 3,
            })
            self.assertEqual(result["user_key_text_concurrency"], 7)
            self.assertEqual(result["user_key_image_concurrency"], 3)
            reloaded = self.config_module.ConfigStore(Path(tmp_dir) / "config.json")
            self.assertEqual(reloaded.user_key_text_concurrency, 7)
            self.assertEqual(reloaded.user_key_image_concurrency, 3)

    def test_invalid_values_fall_back_to_defaults(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = self.make_store(
                tmp_dir,
                user_key_text_concurrency=0,
                user_key_image_concurrency=True,
            )
            self.assertEqual(store.user_key_text_concurrency, 4)
            self.assertEqual(store.user_key_image_concurrency, 1)
            result = store.update({
                "user_key_text_concurrency": "not-an-int",
                "user_key_image_concurrency": -2,
            })
            self.assertEqual(result["user_key_text_concurrency"], 4)
            self.assertEqual(result["user_key_image_concurrency"], 1)
~~~

- [ ] Step 2: Run the focused tests and verify the expected missing-accessor failure

~~~bash
uv run python -m unittest test.test_config.UserKeyConcurrencyConfigTests -v
~~~

Expected: FAIL with missing ConfigStore.user_key_text_concurrency or ConfigStore.user_key_image_concurrency.

- [ ] Step 3: Implement the minimal configuration support

In services/config.py add constants beside the existing scheduler defaults:

~~~python
DEFAULT_USER_KEY_TEXT_CONCURRENCY = 4
DEFAULT_USER_KEY_IMAGE_CONCURRENCY = 1
~~~

Add a helper beside _normalize_positive_int:

~~~python
def _normalize_user_key_concurrency(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        normalized = int(value)
    except (OverflowError, TypeError, ValueError):
        return default
    return normalized if normalized >= 1 else default
~~~

Add ConfigStore properties:

~~~python
@property
def user_key_text_concurrency(self) -> int:
    return _normalize_user_key_concurrency(
        self.data.get("user_key_text_concurrency"),
        DEFAULT_USER_KEY_TEXT_CONCURRENCY,
    )

@property
def user_key_image_concurrency(self) -> int:
    return _normalize_user_key_concurrency(
        self.data.get("user_key_image_concurrency"),
        DEFAULT_USER_KEY_IMAGE_CONCURRENCY,
    )
~~~

In ConfigStore.get expose both normalized properties. At the beginning of ConfigStore.update canonicalize both values in next_data using _normalize_user_key_concurrency, then save as existing code does.

- [ ] Step 4: Run the focused tests

~~~bash
uv run python -m unittest test.test_config.UserKeyConcurrencyConfigTests -v
~~~

Expected: PASS.

- [ ] Step 5: Commit

~~~bash
git add services/config.py test/test_config.py
git commit -m "feat: add user key concurrency settings"
~~~

## Task 2: Build The Thread-Safe Limiter

**Files:**
- Create: test/test_user_key_concurrency.py
- Create: services/user_key_concurrency.py

- [ ] Step 1: Write failing limiter tests

Create tests for text/image isolation, user isolation, admin bypass, idempotent release, dynamic limits, and recovered work:

~~~python
USER = {"id": "user-1", "role": "user"}
OTHER_USER = {"id": "user-2", "role": "user"}
ADMIN = {"id": "admin", "role": "admin"}

def setUp(self):
    self.limits = {"text": 1, "image": 1}
    self.limiter = UserKeyConcurrencyLimiter(
        text_limit_getter=lambda: self.limits["text"],
        image_limit_getter=lambda: self.limits["image"],
    )

def test_text_and_image_limits_are_independent_per_user(self):
    text = self.limiter.try_acquire(USER, "text")
    image = self.limiter.try_acquire(USER, "image")
    self.assertIsNotNone(text)
    self.assertIsNotNone(image)
    self.assertIsNone(self.limiter.try_acquire(USER, "text"))
    self.assertIsNone(self.limiter.try_acquire(USER, "image"))
    self.assertIsNotNone(self.limiter.try_acquire(OTHER_USER, "text"))

def test_admin_is_not_limited(self):
    leases = [self.limiter.try_acquire(ADMIN, "text") for _ in range(20)]
    self.assertTrue(all(lease is not None for lease in leases))

def test_release_is_idempotent(self):
    lease = self.limiter.try_acquire(USER, "text")
    lease.release()
    lease.release()
    self.assertIsNotNone(self.limiter.try_acquire(USER, "text"))
    self.assertEqual(self.limiter.active_count(USER, "text"), 1)

def test_limit_changes_apply_to_new_admissions(self):
    first = self.limiter.try_acquire(USER, "text")
    self.limits["text"] = 2
    second = self.limiter.try_acquire(USER, "text")
    self.limits["text"] = 1
    third = self.limiter.try_acquire(USER, "text")
    self.assertIsNotNone(first)
    self.assertIsNotNone(second)
    self.assertIsNone(third)

def test_recovered_lease_is_counted_even_when_limit_is_full(self):
    first = self.limiter.try_acquire(USER, "image")
    recovered = self.limiter.acquire_existing(USER, "image")
    self.assertIsNotNone(first)
    self.assertIsNotNone(recovered)
    self.assertEqual(self.limiter.active_count(USER, "image"), 2)
    first.release()
    recovered.release()
    self.assertEqual(self.limiter.active_count(USER, "image"), 0)
~~~

- [ ] Step 2: Run the tests and confirm they fail because the module is missing

~~~bash
uv run python -m unittest test.test_user_key_concurrency -v
~~~

Expected: FAIL with ModuleNotFoundError.

- [ ] Step 3: Implement the limiter

Create services/user_key_concurrency.py. Use RequestType = Literal["text", "image"], UserKeyConcurrencyLimitError with request_type, status_code 429, retry_after_seconds 1, an idempotent UserKeyConcurrencyLease protected by its own Lock, and UserKeyConcurrencyLimiter protected by a Lock.

The limiter must implement:

~~~python
def try_acquire(self, identity, request_type):
    if identity.get("role") != "user":
        return UserKeyConcurrencyLease(None, None)
    key = (str(identity.get("id") or "").strip(), request_type)
    if not key[0]:
        return UserKeyConcurrencyLease(None, None)
    limit = max(1, int(self._limit_getters[request_type]()))
    with self._lock:
        active = self._active.get(key, 0)
        if active >= limit:
            return None
        self._active[key] = active + 1
    return UserKeyConcurrencyLease(self, key)
~~~

Also implement acquire_existing, _release, and active_count. acquire_existing increments without checking the limit for service-restarted tasks, while all user-originated admissions use try_acquire.

Create the module-level instance with getters reading config.user_key_text_concurrency and config.user_key_image_concurrency.

- [ ] Step 4: Run the limiter tests

~~~bash
uv run python -m unittest test.test_user_key_concurrency -v
~~~

Expected: PASS.

- [ ] Step 5: Commit

~~~bash
git add services/user_key_concurrency.py test/test_user_key_concurrency.py
git commit -m "feat: add user key concurrency limiter"
~~~

## Task 3: Enforce Limits Around /v1 Requests

**Files:**
- Modify: test/test_quota_request_integration.py
- Modify: api/support.py

- [ ] Step 1: Write failing integration tests

Patch a one-slot UserKeyConcurrencyLimiter into support_module and add tests for a held text slot, compatible 429 response, Retry-After: 1, no handler call, release, and streaming release:

~~~python
def test_text_limit_returns_429_before_handler_and_releases_after_manual_lease(self):
    limiter = UserKeyConcurrencyLimiter(
        text_limit_getter=lambda: 1,
        image_limit_getter=lambda: 1,
    )
    with mock.patch.object(support_module, "user_key_concurrency", limiter):
        held = limiter.try_acquire(USER_IDENTITY, "text")
        try:
            with mock.patch.object(ai_module.openai_search, "handle", return_value={"ok": True}) as handler:
                response = self.client.post("/v1/search", headers=AUTH_HEADERS, json={"prompt": "hi"})
            self.assertEqual(response.status_code, 429, response.text)
            self.assertEqual(response.headers["retry-after"], "1")
            self.assertEqual(response.json()["error"]["code"], "user_key_concurrency_limit")
            handler.assert_not_called()
        finally:
            held.release()

        with mock.patch.object(ai_module.openai_search, "handle", return_value={"ok": True}):
            response = self.client.post("/v1/search", headers=AUTH_HEADERS, json={"prompt": "hi"})
        self.assertEqual(response.status_code, 200, response.text)

def test_streaming_text_releases_user_key_lease_after_response_consumes(self):
    async def stream_handler(_payload):
        yield {"choices": [{"delta": {"content": "one"}}]}

    limiter = UserKeyConcurrencyLimiter(
        text_limit_getter=lambda: 1,
        image_limit_getter=lambda: 1,
    )
    with mock.patch.object(support_module, "user_key_concurrency", limiter):
        with mock.patch.object(ai_module.openai_v1_chat_complete, "handle", stream_handler):
            response = self.client.post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "stream": True, "messages": [{"role": "user", "content": "hi"}]},
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(limiter.active_count(USER_IDENTITY, "text"), 0)

def test_image_generation_uses_image_lease_and_returns_429_when_held(self):
    limiter = UserKeyConcurrencyLimiter(
        text_limit_getter=lambda: 4,
        image_limit_getter=lambda: 1,
    )
    with mock.patch.object(support_module, "user_key_concurrency", limiter):
        held = limiter.try_acquire(USER_IDENTITY, "image")
        try:
            with mock.patch.object(ai_module.openai_image_generation, "handle", return_value={"ok": True}) as handler:
                response = self.client.post(
                    "/v1/images/generations",
                    headers=AUTH_HEADERS,
                    json={"prompt": "hi"},
                )
            self.assertEqual(response.status_code, 429, response.text)
            self.assertEqual(response.headers["retry-after"], "1")
            self.assertEqual(response.json()["error"]["code"], "user_key_concurrency_limit")
            handler.assert_not_called()
        finally:
            held.release()
~~~

- [ ] Step 2: Run the new tests and verify they fail

~~~bash
uv run python -m unittest test.test_quota_request_integration.QuotaRequestIntegrationTests.test_text_limit_returns_429_before_handler_and_releases_after_manual_lease -v
~~~

Expected: FAIL because api.support does not acquire leases.

- [ ] Step 3: Implement support integration

Import user_key_concurrency. Add _user_key_concurrency_response returning an error object with message user key <type> concurrency limit exceeded, type rate_limit_error, and code user_key_concurrency_limit.

At the beginning of run_with_quota, before quota_service.reserve, call try_acquire. If it returns None, raise HTTPException(429, detail=response, headers={"Retry-After": "1"}). If quota reserve fails, release the lease before re-raising the existing quota HTTPException.

Replace the existing accounting callbacks with settle_success and settle_failure wrappers. Each wrapper invokes the matching QuotaAccounting method in a try/finally that releases the lease. The outer BaseException handler calls settle_failure so handler exceptions, scheduler errors, and cancellation also release. Keep accounting.observe as on_stream_item. The idempotent lease makes duplicate failure notifications safe.

- [ ] Step 4: Run all quota integration tests

~~~bash
uv run python -m unittest test.test_quota_request_integration -v
~~~

Expected: PASS.

- [ ] Step 5: Commit

~~~bash
git add api/support.py test/test_quota_request_integration.py
git commit -m "feat: enforce user key limits on api requests"
~~~

## Task 4: Enforce Limits For Asynchronous Image Tasks

**Files:**
- Modify: test/test_image_task_service.py
- Modify: services/image_task_service.py
- Modify: test/test_image_tasks_api.py
- Modify: api/image_tasks.py

- [ ] Step 1: Write failing lifecycle/API tests

Update the existing make_service helper to accept a limiter. Add a blocking-handler test: submit one user task, wait until it starts, assert a second task raises UserKeyConcurrencyLimitError, release the first handler, wait for success, submit the second task, and wait for its success. Add a duplicate-submit test asserting active_count remains one and becomes zero at terminal success. Add an API test where the fake service raises UserKeyConcurrencyLimitError("image") and assert 429, Retry-After 1, and code user_key_concurrency_limit.

Use this test shape:

~~~python
started = Event()
release = Event()

def handler(_payload):
    started.set()
    release.wait(timeout=2)
    return {"data": [{"url": "http://example.test/image.png"}]}

limiter = UserKeyConcurrencyLimiter(
    text_limit_getter=lambda: 4,
    image_limit_getter=lambda: 1,
)
service = self.make_service(path, handler, quota_service=quota, concurrency_limiter=limiter)
service.submit_generation(USER_OWNER, client_task_id="first", prompt="cat", model="gpt-image-2", size=None)
self.assertTrue(started.wait(timeout=2))
with self.assertRaises(UserKeyConcurrencyLimitError):
    service.submit_generation(USER_OWNER, client_task_id="second", prompt="cat", model="gpt-image-2", size=None)
release.set()
wait_for_task(service, USER_OWNER, "first", "success")
~~~

- [ ] Step 2: Run focused tests and verify they fail

~~~bash
uv run python -m unittest \
  test.test_image_task_service.ImageTaskServiceTests.test_user_image_tasks_hold_one_image_lease_until_terminal_state \
  test.test_image_tasks_api.ImageTasksApiTests.test_concurrency_exhaustion_returns_retryable_429 -v
~~~

Expected: FAIL because ImageTaskService has no limiter injection and image_tasks does not catch the exception.

- [ ] Step 3: Implement task lease state and rollback

Add a concurrency_limiter optional constructor parameter, defaulting to the module-level limiter, and initialize _concurrency_leases keyed by task key.

Implement _acquire_task_concurrency and _release_task_concurrency. The acquire helper calls try_acquire(identity, "image") and raises UserKeyConcurrencyLimitError("image") on None. The release helper pops the lease under the service lock and calls release outside the lock.

In _submit, keep the duplicate task lookup before acquiring. Acquire immediately before _reserve_task_quota. Release on quota failure, state persistence failure, and dispatch failure. Add release in the finally blocks of _run_task and _run_resume_poll, plus admission/background watcher error paths.

In resume_poll, acquire a fresh image lease after task validation and quota renewal and before setting running. Release if dispatch fails. During start recovery, acquire_existing using the persisted owner ID with role user before dispatching each recovered task; this counts recovered work without letting the internal scheduler identity bypass the limit.

- [ ] Step 4: Map the exception to image-task API errors

Catch UserKeyConcurrencyLimitError before SchedulerError in the generation, edit, and resume-poll endpoints:

~~~python
except UserKeyConcurrencyLimitError as exc:
    raise HTTPException(
        status_code=429,
        detail={
            "error": {
                "message": str(exc),
                "type": "rate_limit_error",
                "code": "user_key_concurrency_limit",
            }
        },
        headers={"Retry-After": str(exc.retry_after_seconds)},
    ) from exc
~~~

- [ ] Step 5: Run image-task tests

~~~bash
uv run python -m unittest test.test_image_task_service test.test_image_tasks_api -v
~~~

Expected: PASS, including existing quota persistence/recovery tests.

- [ ] Step 6: Commit

~~~bash
git add services/image_task_service.py api/image_tasks.py test/test_image_task_service.py test/test_image_tasks_api.py
git commit -m "feat: limit user key image tasks"
~~~

## Task 5: Add Web Settings Controls

**Files:**
- Modify: web/src/lib/api.ts
- Modify: web/src/app/settings/store.ts
- Modify: web/src/app/settings/components/config-card.tsx

- [ ] Step 1: Add typed fields and store declarations

Extend SettingsConfig:

~~~typescript
user_key_text_concurrency?: number | string;
user_key_image_concurrency?: number | string;
~~~

Add to normalizeConfig:

~~~typescript
user_key_text_concurrency: Math.max(1, Number(config.user_key_text_concurrency) || 4),
user_key_image_concurrency: Math.max(1, Number(config.user_key_image_concurrency) || 1),
~~~

Add to SettingsStore:

~~~typescript
setUserKeyTextConcurrency: (value: string) => void;
setUserKeyImageConcurrency: (value: string) => void;
~~~

- [ ] Step 2: Run the frontend type check before completing implementations

~~~bash
cd web && bunx tsc --noEmit
~~~

Expected: FAIL if the new fields are referenced before store implementations and component bindings exist.

- [ ] Step 3: Implement store normalization, save payload, and setters

Add the two canonical fields to saveConfig:

~~~typescript
user_key_text_concurrency: Math.max(1, Number(config.user_key_text_concurrency) || 4),
user_key_image_concurrency: Math.max(1, Number(config.user_key_image_concurrency) || 1),
~~~

Add setter implementations beside setImageAccountConcurrency:

~~~typescript
setUserKeyTextConcurrency: (value) => {
  set((state) => state.config ? { config: { ...state.config, user_key_text_concurrency: value } } : {});
},
setUserKeyImageConcurrency: (value) => {
  set((state) => state.config ? { config: { ...state.config, user_key_image_concurrency: value } } : {});
},
~~~

- [ ] Step 4: Add number inputs to ConfigCard

Select both setters and add two sibling form blocks near the existing image concurrency input. Use type number, min 1, step 1, placeholders 4 and 1, and descriptions stating that the limit is per ordinary user key and administrators are exempt:

~~~tsx
<div className="space-y-2">
  <label className="text-sm text-stone-700">用户密钥文本并发</label>
  <Input type="number" min={1} step={1}
    value={String(config?.user_key_text_concurrency || "")}
    onChange={(event) => setUserKeyTextConcurrency(event.target.value)}
    placeholder="4"
    className="h-10 rounded-xl border-stone-200 bg-white" />
  <p className="text-xs text-stone-500">每个普通用户密钥同时进行的文本请求数量，默认 4；管理员密钥不受限制。</p>
</div>
<div className="space-y-2">
  <label className="text-sm text-stone-700">用户密钥图片并发</label>
  <Input type="number" min={1} step={1}
    value={String(config?.user_key_image_concurrency || "")}
    onChange={(event) => setUserKeyImageConcurrency(event.target.value)}
    placeholder="1"
    className="h-10 rounded-xl border-stone-200 bg-white" />
  <p className="text-xs text-stone-500">每个普通用户密钥同时进行的图片请求和图片任务数量，默认 1；管理员密钥不受限制。</p>
</div>
~~~

- [ ] Step 5: Run frontend checks

~~~bash
cd web && bunx tsc --noEmit && bun run build
~~~

Expected: TypeScript and Next.js build both pass.

- [ ] Step 6: Commit

~~~bash
git add web/src/lib/api.ts web/src/app/settings/store.ts web/src/app/settings/components/config-card.tsx
git commit -m "feat: expose user key concurrency settings"
~~~

## Task 6: Verify Regressions And Final Diff

- [ ] Step 1: Run the complete Python suite

~~~bash
uv run pytest -q
~~~

Expected: all tests pass, especially config, quota integration, image-task, and scheduler tests.

- [ ] Step 2: Inspect final diff and worktree

~~~bash
git diff --check HEAD~6..HEAD
git status --short --branch
git diff --stat HEAD~6..HEAD
~~~

Expected: only planned files are committed; the pre-existing config.json remains modified and uncommitted.

- [ ] Step 3: If verification exposes a regression, add a named failing test to the affected existing test file, apply the smallest fix in the corresponding implementation file, rerun that focused test and the full suite, then commit only those two concrete files with a message describing the regression.
