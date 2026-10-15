# Sub2API Automatic Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 为每个 Sub2API 连接增加可持久化的自动同步周期，自动读取全部远端 OpenAI OAuth 账号并安全导入本地账号池，同时保证手动导入与自动同步互斥、失败可重试且本地账号不被远端删除影响。

**Architecture:** 在现有 Sub2APIConfig JSON 配置上增加周期和同步状态，在现有导入服务上增加按连接的共享操作锁和异常收敛。新增 Sub2APIAutoSyncService，由一个协调线程和有界执行池负责到期调度；它在获得连接锁后读取远端全量账号、启动现有导入任务并等待终态。API 只负责校验和通知调度器，设置页保存配置并轮询汇总状态。

**Tech Stack:** Python 3.13, FastAPI, Pydantic, threading, concurrent.futures.ThreadPoolExecutor, existing JSON configuration and account service, Next.js/React/TypeScript, pytest/unittest.

---

## 文件结构

- Create: services/sub2api_auto_sync_service.py，自动同步调度、有限并发、周期计算、状态更新和关闭逻辑。
- Create: test/test_sub2api_service.py，配置规范化、凭据导出分批、共享锁和导入异常测试。
- Create: test/test_sub2api_auto_sync.py，自动同步服务的确定性运行、周期、失败、重试和并发测试。
- Create: test/test_sub2api_api.py，Sub2API 配置 API 的请求校验、立即触发和忙碌状态码测试。
- Modify: services/sub2api_service.py，增加自动同步字段、深复制读取、导出分批、每连接锁、导入终态和错误收敛。
- Modify: api/accounts.py，接受同步周期、通知调度器，并将导入忙碌映射为 HTTP 409。
- Modify: api/app.py，在应用 lifespan 启动和停止自动同步服务。
- Modify: web/src/lib/api.ts，增加同步状态类型和配置字段。
- Modify: web/src/app/settings/components/sub2api-connections.tsx，增加周期输入、状态展示和自动任务轮询。

现有 api/support.py 的脱敏函数会保留新增的非敏感字段，因此不需要修改。现有 data/sub2api_config.json 通过缺省值兼容，不需要迁移脚本或第三方依赖。

后端测试使用 uv run pytest。若宿主机没有 uv，在项目已有 Python 容器中执行同样的命令。前端验证在 web/ 目录执行 tsc、定向 ESLint 和 next build。

## Task 1: Extend Sub2API configuration and bounded credential export

Files:
- Modify: services/sub2api_service.py，配置规范化和 _fetch_access_tokens_for_accounts
- Create: test/test_sub2api_service.py

- [ ] Step 1: Write failing configuration and batching tests.

使用 TemporaryDirectory 创建独立配置文件，不触碰仓库 data 目录。覆盖旧配置默认关闭、新增和更新周期、状态持久化、非法周期归零，以及 125 个账号被拆成两个最多 100 个 ID 的导出请求。

测试辅助对象应包含：

```python
def make_server(server_id="server-1"):
    return {
        "id": server_id,
        "name": "test",
        "base_url": "https://sub2api.example.test",
        "email": "admin@example.test",
        "password": "secret",
        "api_key": "",
        "group_id": "",
    }

def test_old_config_defaults_to_disabled_auto_sync(self):
    path = self.temp_path / "sub2api.json"
    path.write_text(json.dumps([make_server()]), encoding="utf-8")
    config = Sub2APIConfig(path)
    server = config.get_server("server-1")
    self.assertEqual(server["auto_sync_interval_minutes"], 0)
    self.assertEqual(server["auto_sync"]["status"], "disabled")
    self.assertIsNone(server["auto_sync"]["next_run_at"])

def test_set_auto_sync_state_persists_only_existing_server(self):
    config = Sub2APIConfig(self.temp_path / "sub2api.json")
    added = config.add_server(
        name="test", base_url="https://sub2api.example.test",
        email="admin@example.test", password="secret", api_key="", group_id="",
        auto_sync_interval_minutes=5,
    )
    result = config.set_auto_sync_state(added["id"], status="running", last_error="")
    self.assertEqual(result["auto_sync"]["status"], "running")
    self.assertIsNone(config.set_auto_sync_state("missing", status="running"))
```

用 mock.patch 替换 services.sub2api_service.Session，记录每次请求的 params["ids"]，并返回每个请求 ID 对应的 access_token。断言返回 125 个 token、无错误、请求次数为 2 且每批不超过 100 个 ID。

- [ ] Step 2: Run focused tests and verify red.

```bash
uv run pytest -q test/test_sub2api_service.py
```

Expected: collection or assertion failures because the automatic-sync fields, state setter, new add_server argument, and export batching do not exist.

- [ ] Step 3: Implement configuration normalization and state access.

在 services/sub2api_service.py 增加 AUTO_SYNC_STATUSES、MAX_IMPORT_ERRORS 和 _normalize_interval。周期只接受大于零的整数；bool、小数、负数、无法转换的值统一规范化为 0。增加 _normalize_auto_sync，返回以下固定字段：

```python
{
    "status": "disabled" | "idle" | "running" | "completed" | "failed",
    "last_started_at": str | None,
    "last_finished_at": str | None,
    "last_success_at": str | None,
    "next_run_at": str | None,
    "last_error": str,
    "last_remote_count": int,
    "last_import_job_id": str,
    "last_import_failed": int,
}
```

扩展 _normalize_server、add_server 和 update_server。add_server 的新参数为 auto_sync_interval_minutes: int = 0。增加 set_auto_sync_state(server_id, **updates)，在现有配置锁内合并嵌套状态、保存文件并返回深复制；连接不存在时返回 None。list_servers 和 get_server 也返回深复制，避免后台线程绕过配置锁修改嵌套字典。

旧配置缺少新增字段时使用关闭状态。加载状态时保留 running 作为运行状态，进程重启恢复由 Task 3 的调度器显式处理，避免配置层误报任务完成。

- [ ] Step 4: Implement bounded credential export.

将 _fetch_access_tokens_for_accounts 的单次 ids 请求改成固定批次，每批最多 100 个 ID，保留现有 API 路径、认证头、timezone=Asia/Shanghai 和错误格式。所有 Session 必须在 finally 中关闭。跨批次聚合 tokens 和 errors；单批 HTTP 或响应格式异常继续抛出 RuntimeError，由导入服务记录该批次失败。调用方负责去重，导出函数不删除本地账号，也不记录任何凭据。

- [ ] Step 5: Run focused tests and verify green.

```bash
uv run pytest -q test/test_sub2api_service.py
```

Expected: old configuration loads with interval zero, valid values round-trip, invalid values normalize to zero, state updates persist, and batched export returns all tokens.

- [ ] Step 6: Commit the configuration and export changes.

```bash
git add services/sub2api_service.py test/test_sub2api_service.py
git commit -m "feat: add sub2api sync configuration and batched export"
```

## Task 2: Make manual and automatic imports share one per-connection operation lock

Files:
- Modify: services/sub2api_service.py，Sub2APIImportService
- Modify: test/test_sub2api_service.py

- [ ] Step 1: Write failing locking and failure-convergence tests.

使用事件控制第一个导入任务，断言同一连接的第二个 start_import 抛出 Sub2APISyncBusyError，而不同连接可以同时启动。再覆盖导出异常后任务进入 failed、锁释放、第二次导入可以启动，以及 100 个账号错误只保留最后 50 条但 failed 仍为 100。

```python
def test_second_import_for_same_server_is_rejected_while_first_runs(self):
    started = threading.Event()
    release = threading.Event()

    def fetch(_server, _ids):
        started.set()
        release.wait(timeout=2)
        return ["token-1"], []

    with mock.patch(
        "services.sub2api_service._fetch_access_tokens_for_accounts",
        side_effect=fetch,
    ):
        first = self.import_service.start_import(make_server(), ["remote-1"])
        self.assertTrue(started.wait(timeout=1))
        with self.assertRaises(Sub2APISyncBusyError):
            self.import_service.start_import(make_server(), ["remote-2"])
        release.set()
        job = self.import_service.wait_for_job("server-1", first["job_id"], timeout=2)
        self.assertEqual(job["status"], "completed")
```

- [ ] Step 2: Run focused tests and verify red.

```bash
uv run pytest -q test/test_sub2api_service.py
```

Expected: the busy exception, per-server lock, wait_for_job, and exception-safe terminal transition are absent.

- [ ] Step 3: Add the shared operation lock and terminal wait API.

Add the following public error and internal state:

```python
class Sub2APISyncBusyError(RuntimeError):
    code = "sub2api_sync_busy"

class Sub2APIImportService:
    def __init__(self, sub2api_config):
        self._config = sub2api_config
        self._locks = {}
        self._locks_lock = threading.Lock()
        self._job_events = {}
```

Implement acquire_operation(server_id) as a context manager using a per-server nonblocking Lock. Manual start_import acquires the lock and transfers ownership to the worker. Automatic sync will hold the context manager across remote listing and import, then call start_import(server, account_ids, operation_lock_held=True). Release the lock in every setup failure and worker terminal path. Keep the lock map bounded by removing an unlocked lock after no active job references it.

Implement wait_for_job(server_id, job_id, timeout=None, stop_event=None) with a per-job Event. _update_job sets the event when status is completed or failed. Terminal jobs loaded from configuration return immediately; timeout raises TimeoutError without changing persisted state.

- [ ] Step 4: Make _run_import exception-safe and preserve bounded errors.

Wrap fetch, account_service.add_accounts, account_service.refresh_accounts, and result construction in try/except/finally. Any exception appends one bounded error, marks the job failed, and releases the operation lock. _append_error increments failed independently and retains only MAX_IMPORT_ERRORS records. A partial import may remain completed for the existing manual UI, but failed must equal the number of failed accounts; the automatic scheduler will treat failed > 0 as an unsuccessful cycle. If a server was deleted, state updates become no-ops but the lock still releases.

- [ ] Step 5: Run focused tests and verify green.

```bash
uv run pytest -q test/test_sub2api_service.py
```

Expected: same-server overlap is rejected, different servers run independently, terminal wait works, exceptions release locks, and errors are bounded without losing counts.

- [ ] Step 6: Commit the shared import operation changes.

```bash
git add services/sub2api_service.py test/test_sub2api_service.py
git commit -m "feat: serialize sub2api imports per connection"
```

## Task 3: Implement the in-process automatic sync scheduler

Files:
- Create: services/sub2api_auto_sync_service.py
- Create: test/test_sub2api_auto_sync.py

- [ ] Step 1: Write deterministic scheduler tests.

Inject a temporary Sub2APIConfig, a fake import service, and a fake list_accounts callable. Use run_once(server_id, now=1700000000.0) for deterministic tests. Cover disabled connections, all remote IDs with duplicate removal, empty remote lists, busy skip preserving last_error, list failure retry, partial import failure, immediate reconfiguration, zero disabling, persisted running recovery, and different-connection execution.

The central assertions are:

```python
list_accounts.return_value = [{"id": "a"}, {"id": "b"}, {"id": "a"}]
imports.wait_for_job.return_value = {
    "status": "completed",
    "failed": 0,
    "job_id": "job-1",
    "added": 1,
    "skipped": 1,
    "refreshed": 1,
}
self.assertTrue(service.run_once("server-1", now=1700000000.0))
imports.start_import.assert_called_once_with(
    mock.ANY, ["a", "b"], operation_lock_held=True,
)
state = config.get_server("server-1")["auto_sync"]
self.assertEqual(state["status"], "completed")
self.assertEqual(state["last_remote_count"], 3)
self.assertEqual(state["last_import_job_id"], "job-1")
```

A busy lock must not call list_accounts, must keep the previous last_error, and must only set a future next deadline. Empty remote lists are successful and must not call any local deletion method. A job with failed > 0 produces automatic status failed even if job status is completed.

- [ ] Step 2: Run scheduler tests and verify red.

```bash
uv run pytest -q test/test_sub2api_auto_sync.py
```

Expected: collection fails because services/sub2api_auto_sync_service.py does not exist.

- [ ] Step 3: Implement scheduler lifecycle and bounded execution.

Create Sub2APIAutoSyncService with this public surface:

```python
class Sub2APIAutoSyncService:
    def __init__(
        self,
        config,
        import_service,
        list_accounts=list_remote_accounts,
        *,
        max_workers=8,
    ): pass
    def start(self, stop_event): pass
    def stop(self, timeout=2.0): pass
    def wake(self): pass
    def notify_server_changed(self, server_id, *, immediate, now=None): pass
    def server_deleted(self, server_id): pass
    def run_once(self, server_id, *, now=None): pass
```

Use one coordinator Thread, a wake Event, ThreadPoolExecutor(max_workers), an active-server set, and a lock protecting that set. Before submit, reject a due server when the active count reaches max_workers; mark its next cycle without placing a task in the executor queue. Remove every submitted server from the active set in a finally block.

Use UTC epoch seconds for due comparisons and UTC ISO 8601 strings for persisted fields. Missing or invalid next_run_at is due immediately. A positive interval with a future deadline waits. On startup, persisted running states become failed with error previous automatic sync was interrupted by process restart and an immediate deadline. stop wakes the coordinator, prevents new submissions, joins it, and shuts down the executor without starting replacement work.

- [ ] Step 4: Implement run_once and state transitions.

The operation order must be:

```python
server = config.get_server(server_id)
if server is None or interval <= 0:
    return False
try:
    operation = import_service.acquire_operation(server_id)
except Sub2APISyncBusyError:
    schedule_next_cycle(server_id)
    log_skip(server_id, "connection_busy")
    return False

with operation:
    set_running(server_id)
    try:
        remote_accounts = list_accounts(server)
        ids = list(dict.fromkeys(
            str(item.get("id") or "").strip()
            for item in remote_accounts
            if str(item.get("id") or "").strip()
        ))
        config.set_auto_sync_state(server_id, last_remote_count=len(remote_accounts))
        if not ids:
            mark_success(server_id, job=None)
            return True
        job = import_service.start_import(
            server, ids, operation_lock_held=True
        )
        config.set_auto_sync_state(
            server_id, last_import_job_id=job["job_id"]
        )
        result = import_service.wait_for_job(server_id, job["job_id"])
        if result["status"] != "completed" or int(result["failed"] or 0) > 0:
            raise RuntimeError(job_error(result))
        mark_success(server_id, job=result)
        return True
    except Exception as exc:
        mark_failure(server_id, str(exc) or "automatic sync failed")
        return False
```

_mark_success clears last_error, writes last_success_at and last_finished_at, records import counts, and schedules the next run using the current positive interval. _mark_failure preserves remote count and job ID, writes last_finished_at, last_error, failed count, and schedules the next run. If a settings update created a newer immediate deadline while a run was active, completion must preserve that newer deadline instead of overwriting it.

Never call a local account deletion method. Log only started, completed, failed, and skipped structured events with server ID, stage, counts, duration, and next deadline; exclude credentials and tokens.

- [ ] Step 5: Run scheduler and service tests.

```bash
uv run pytest -q test/test_sub2api_auto_sync.py test/test_sub2api_service.py
```

Expected: all deterministic scheduler tests pass, including full import, empty remote list, busy skip, preserved errors, partial failure, restart recovery, bounded executor behavior, and next-cycle scheduling.

- [ ] Step 6: Commit the automatic scheduler.

```bash
git add services/sub2api_auto_sync_service.py test/test_sub2api_auto_sync.py
git commit -m "feat: add sub2api automatic sync scheduler"
```

## Task 4: Connect API endpoints and application lifespan

Files:
- Modify: api/accounts.py，Sub2API request models and endpoints
- Modify: api/app.py，FastAPI lifespan
- Create: test/test_sub2api_api.py

- [ ] Step 1: Write failing API tests.

Use a router with accounts.create_router(), temporary config, fake import service, and fake auto-sync service. With an admin bearer header, cover creation with interval 5, negative interval returning 422, update preserving an omitted interval, relevant update calling immediate=True, name-only update not triggering a run, busy manual import returning 409, deletion notification, non-admin 403, and secret sanitization.

The busy assertion is:

```python
fake_import.start_import.side_effect = Sub2APISyncBusyError("busy")
response = client.post(
    "/api/sub2api/servers/server-1/import",
    headers=admin_headers,
    json={"account_ids": ["remote-1"]},
)
assert response.status_code == 409
assert response.json()["detail"]["code"] == "sub2api_sync_busy"
```

- [ ] Step 2: Run API tests and verify red.

```bash
uv run pytest -q test/test_sub2api_api.py
```

Expected: request validation and fake-service assertions fail because the Pydantic field and scheduler hooks are not wired.

- [ ] Step 3: Extend request models and endpoint wiring.

Add auto_sync_interval_minutes: int = Field(default=0, ge=0) to the create model and auto_sync_interval_minutes: int | None = Field(default=None, ge=0) to the update model. Pass the create value into add_server. After create call notify_server_changed with immediate=True when the resulting interval is positive.

For update, compute whether base_url, email, password, api_key, group_id, or interval changed. If a sync-relevant field changed and the resulting interval is positive, call notify_server_changed(server_id, immediate=True). A name-only update must not trigger an immediate run. If the resulting interval is zero, call notify_server_changed(server_id, immediate=False) so future work is disabled.

Catch Sub2APISyncBusyError before ValueError in the manual import endpoint and return status 409 with detail error and code sub2api_sync_busy. Keep empty IDs at 400, unknown servers at 404, and use existing sanitization so password and api_key never appear in responses.

On delete call server_deleted after removing the config. A deleted active task may finish its network call, but its state writes must be no-ops.

- [ ] Step 4: Start and stop the scheduler in the FastAPI lifespan.

Import sub2api_auto_sync_service in api/app.py. Start it after existing service initialization and before yield:

```python
sub2api_auto_sync_service.start(stop_event)
```

In finally, call stop immediately after stop_event.set(). Shutdown must wake the coordinator, stop new submissions, join the coordinator, and not leave a scheduler thread running after the lifespan exits.

- [ ] Step 5: Run API, scheduler, and adjacent account tests.

```bash
uv run pytest -q test/test_sub2api_api.py test/test_sub2api_auto_sync.py test/test_sub2api_service.py test/test_account_export.py
```

Expected: new API behavior passes, secrets stay sanitized, and existing account behavior remains unchanged.

- [ ] Step 6: Commit API and lifespan integration.

```bash
git add api/accounts.py api/app.py test/test_sub2api_api.py
git commit -m "feat: expose sub2api automatic sync settings"
```

## Task 5: Add Web configuration and live status display

Files:
- Modify: web/src/lib/api.ts，Sub2API types and request payloads
- Modify: web/src/app/settings/components/sub2api-connections.tsx，form, cards, polling, and busy error

- [ ] Step 1: Extend client types and API payloads.

Add the exact server shape:

```typescript
export type Sub2APIAutoSync = {
  status: "disabled" | "idle" | "running" | "completed" | "failed";
  last_started_at: string | null;
  last_finished_at: string | null;
  last_success_at: string | null;
  next_run_at: string | null;
  last_error: string;
  last_remote_count: number;
  last_import_job_id: string;
  last_import_failed: number;
};

export type Sub2APIServer = {
  id: string;
  name: string;
  base_url: string;
  email: string;
  has_api_key: boolean;
  group_id: string;
  auto_sync_interval_minutes: number;
  auto_sync: Sub2APIAutoSync;
  import_job?: CPAImportJob | null;
};
```

Add auto_sync_interval_minutes to createSub2APIServer and as an optional field to updateSub2APIServer. Send zero explicitly when disabled.

- [ ] Step 2: Add interval form state and validation.

Add formAutoSyncInterval as a string state, reset to "0" for a new connection, and load String(server.auto_sync_interval_minutes ?? 0) while editing. Before saving, require a base-10 integer greater than or equal to zero and show 自动同步间隔必须是大于等于 0 的整数 for invalid values.

Insert a numeric input with min 0 and step 1:

```tsx
<Input
  type="number"
  min={0}
  step={1}
  value={formAutoSyncInterval}
  onChange={(event) => setFormAutoSyncInterval(event.target.value)}
  placeholder="0 表示关闭"
  className="h-11 rounded-xl border-stone-200 bg-white"
/>
```

Include the parsed value in both create and update requests. Keep existing authentication and group behavior unchanged.

- [ ] Step 3: Add connection-card status.

For every connection show the enabled period, auto_sync.status, last remote count, next run time, and a truncated last_error when present. Use existing Badge, LoaderCircle, and text styles without adding a redundant nested card. Show a spinner during running and a failure badge plus error summary during failed.

Change the polling predicate so it polls while either import_job.status is pending/running or auto_sync.status is running. Continue using fetchSub2APIServers only; never list remote accounts during status polling. Preserve effect cleanup so only one interval exists.

- [ ] Step 4: Handle manual 409 without repeated submission.

When startSub2APIImport rejects with HTTP 409, show the backend message and keep the account browser open. Do not retry automatically. For other errors preserve the existing toast behavior.

- [ ] Step 5: Run frontend verification.

```bash
cd web
./node_modules/.bin/tsc --noEmit
./node_modules/.bin/eslint src/lib/api.ts src/app/settings/components/sub2api-connections.tsx
./node_modules/.bin/next build
```

Expected: TypeScript, targeted ESLint, and production build pass.

- [ ] Step 6: Commit the Web changes.

```bash
git add web/src/lib/api.ts web/src/app/settings/components/sub2api-connections.tsx
git commit -m "feat: add sub2api auto sync controls"
```

## Task 6: Full verification and inline review

Files:
- Modify only if a focused regression test proves a defect in the implementation files above.

- [ ] Step 1: Run the new backend suite from a clean state.

```bash
uv run pytest -q test/test_sub2api_service.py test/test_sub2api_auto_sync.py test/test_sub2api_api.py
```

Expected: all new Sub2API tests pass.

- [ ] Step 2: Run adjacent regression tests.

```bash
uv run pytest -q test/test_account_export.py test/test_backup_service.py test/test_image_task_service.py test/test_image_tasks_api.py test/test_system_api.py
```

Expected: existing account, background-service, and API tests pass without a configured Sub2API server.

- [ ] Step 3: Run the complete backend suite.

```bash
uv run pytest -q
```

Expected: the full repository suite passes. If the host lacks uv, execute the same command in the repository's existing Docker test environment and record that command in the final report.

- [ ] Step 4: Perform inline behavioral and security review.

Run:

```bash
git diff --check HEAD~5..HEAD
git diff --stat HEAD~5..HEAD
git status --short --branch
```

Inspect the complete diff and verify positive intervals trigger immediate runs after relevant changes, zero disables future work, failures retain errors and retry, busy cycles do not clear errors or create queues, the automatic path never deletes local accounts, manual and automatic paths share one lock, deletion and restart cannot leave running state forever, secrets and access tokens never reach the Web or logs, and unrelated scheduler/account/image/log behavior is unchanged.

- [ ] Step 5: Commit any verification-only correction.

If the review finds a defect, first add a focused regression test, then fix the smallest file, rerun the focused test and complete suite, and commit:

```bash
git add services/sub2api_service.py services/sub2api_auto_sync_service.py api/accounts.py api/app.py web/src/lib/api.ts web/src/app/settings/components/sub2api-connections.tsx test/test_sub2api_service.py test/test_sub2api_auto_sync.py test/test_sub2api_api.py
git commit -m "fix: harden sub2api automatic sync"
```

If no defect is found, leave the implementation commits unchanged and report all fresh verification commands and results.
