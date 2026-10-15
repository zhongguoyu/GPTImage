# SQLite Log System Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`[ ]`) syntax for tracking.

**Goal:** Replace the append-only JSONL log store with a high-performance SQLite log store, reduce routine log volume, keep the existing admin log workflow, and prevent synchronous log queries from blocking the FastAPI event loop.

**Architecture:** Add a dedicated SQLiteLogStore using the stdlib sqlite3 module, WAL mode, synchronous=NORMAL, a busy timeout, indexed timestamp/type queries, short transactions, and a thread-local connection per worker thread. Keep LogService as the application-facing facade so existing callers remain stable. Migrate valid records from data/logs.jsonl once at startup, rename the source to a .migrated backup only after a successful import, and retain logs according to a configurable 30-day default. Serve paginated log data through run_in_threadpool, and make the frontend consume server-side pagination.

**Tech Stack:** Python 3, FastAPI, stdlib sqlite3, existing unittest/pytest test suite, TypeScript/React frontend, Bun build.

---

### Task 1: Add failing SQLite storage contract tests

**Files:**
- Create: test/test_log_store.py
- Reference: docs/superpowers/specs/2026-09-07-sqlite-log-system-design.md

- [ ] **Step 1: Write the failing tests for schema, insert, filtering, pagination, and deletion**

Create tests around a temporary database and a temporary legacy path. The test fixture should instantiate the planned public API:

~~~python
from services.log_store import SQLiteLogStore

store = SQLiteLogStore(db_path, legacy_path)
store.initialize()
store.add("call", "调用完成", {"status": 200})
page = store.query(type_filter="call", page=1, page_size=20)
~~~

Cover these behaviors:

~~~python
def test_initialize_creates_schema_and_pragmas():
    store.initialize()
    row = store.raw_query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'logs'")
    assert row is not None
    assert store.raw_pragma("journal_mode") == "wal"
    assert store.raw_pragma("foreign_keys") == 1

def test_query_returns_newest_first_and_has_total_and_has_more():
    for index in range(3):
        store.add("call", f"call-{index}", {"index": index})
    result = store.query(page=1, page_size=2)
    assert result["total"] == 3
    assert result["has_more"] is True
    assert [item["summary"] for item in result["items"]] == ["call-2", "call-1"]

def test_type_and_date_filters_are_applied_by_sql():
    store.add("call", "call", {})
    store.add("account", "account", {})
    result = store.query(type_filter="account", start_date="2099-01-01", end_date="2099-01-31")
    assert result["total"] == 0

def test_delete_by_ids_is_transactional_and_idempotent():
    item = store.add("call", "delete me", {})
    assert store.delete([item["id"], "missing-id"]) == 1
    assert store.delete([item["id"]]) == 0
~~~

The concrete test helpers may expose read-only test inspection methods or use a connection returned by a test-only fixture; do not make production callers depend on private SQL. Assert that the detail object round-trips as JSON and that the generated id, time, and sequence values are present.

- [ ] **Step 2: Run the focused tests and verify the failure is about the missing implementation**

Run:

~~~bash
.venv/bin/python -m pytest test/test_log_store.py -q
~~~

Expected: collection fails with ModuleNotFoundError: No module named services.log_store before any production implementation exists. If the failure is a test typo or environment error, correct that first.

### Task 2: Implement the SQLite schema and query primitives

**Files:**
- Create: services/log_store.py
- Test: test/test_log_store.py

- [ ] **Step 1: Implement connection setup and schema initialization**

Add a SQLiteLogStore with a database path, legacy JSONL path, an initialization lock, and thread-local connections. Each connection must execute:

~~~python
connection = sqlite3.connect(str(self.db_path), timeout=5.0)
connection.row_factory = sqlite3.Row
connection.execute("PRAGMA journal_mode=WAL")
connection.execute("PRAGMA synchronous=NORMAL")
connection.execute("PRAGMA busy_timeout=5000")
connection.execute("PRAGMA foreign_keys=ON")
~~~

initialize() must create the approved schema and indexes with CREATE TABLE IF NOT EXISTS and CREATE INDEX IF NOT EXISTS, then commit:

~~~sql
CREATE TABLE IF NOT EXISTS logs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    time TEXT NOT NULL,
    created_ts REAL NOT NULL,
    type TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_logs_time
    ON logs(created_ts DESC, sequence DESC);

CREATE INDEX IF NOT EXISTS idx_logs_type_time
    ON logs(type, created_ts DESC, sequence DESC);

CREATE TABLE IF NOT EXISTS log_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
~~~

Keep all SQL in this module. Use parameter binding for values and never concatenate user-provided filters into SQL.

- [ ] **Step 2: Implement insert and row decoding**

Add add(log_type, summary, detail, *, record_id=None, recorded_at=None, created_ts=None) returning the public log dictionary. Serialize only a JSON object into detail_json with ensure_ascii=False and compact separators. Generate UUIDs when record_id is absent. Use a single short transaction per insert and preserve stable time text plus a numeric created_ts for indexed range queries.

The decoder must return the existing public shape:

~~~python
{
    "id": row["id"],
    "time": row["time"],
    "type": row["type"],
    "summary": row["summary"],
    "detail": json.loads(row["detail_json"]),
}
~~~

Treat malformed stored detail_json as {} when reading so one damaged detail payload cannot break the entire log page.

- [ ] **Step 3: Implement validated SQL pagination and deletion**

Implement query(type_filter="", start_date="", end_date="", page=1, page_size=20) with page bounds page >= 1 and 1 <= page_size <= 100. Convert dates to local-midnight numeric timestamp bounds; make end_date exclusive by adding one day. Run a count query and a page query in one read transaction, ordered by created_ts DESC, sequence DESC, with LIMIT and OFFSET.

Return:

~~~python
{
    "items": items,
    "total": total,
    "page": page,
    "page_size": page_size,
    "has_more": page * page_size < total,
}
~~~

Implement delete(ids) with one transaction and chunked IN parameters so the method remains safe if a future caller supplies more than SQLite's variable limit. Return the number of deleted rows. Empty input must return 0 without opening a write transaction.

- [ ] **Step 4: Run the focused tests and commit the storage primitive**

Run:

~~~bash
.venv/bin/python -m pytest test/test_log_store.py -q
~~~

Expected: all schema, round-trip, filtering, ordering, pagination, and deletion tests pass.

Commit:

~~~bash
git add services/log_store.py test/test_log_store.py
git commit -m "feat: add sqlite log storage"
~~~

### Task 3: Add migration and retention tests, then implement them

**Files:**
- Modify: test/test_log_store.py
- Modify: services/log_store.py

- [ ] **Step 1: Write failing migration and retention tests**

Add a legacy file containing valid records, an id-less record, a malformed JSON line, and a non-object JSON value. Assert that initialization imports only valid objects, preserves existing IDs, derives a deterministic ID for missing IDs, records migration counts in log_metadata, and renames the source only after import:

~~~python
legacy.write_text(
    json.dumps({"id": "old-1", "time": "2026-09-01 10:00:00", "type": "call", "summary": "old", "detail": {}})
    + "\\n"
    + json.dumps({"time": "2026-09-01 10:01:00", "type": "account", "summary": "legacy", "detail": {}})
    + "\\nnot-json\\n"
    + "[]\\n",
    encoding="utf-8",
)
store.initialize()
assert store.query(page_size=100)["total"] == 2
assert not legacy.exists()
assert (tmp_path / "logs.jsonl.migrated").exists()
assert store.migration_metadata()["invalid_lines"] == 2
store.initialize()
assert store.query(page_size=100)["total"] == 2
~~~

Also test that an existing .migrated backup receives a timestamped suffix, that migration does not rename the source when the SQLite transaction fails, and that retention deletes rows older than the configured cutoff without deleting newer rows. Patch the store clock in the retention test so the age boundary is deterministic.

- [ ] **Step 2: Run the new tests and verify they fail before migration/retention code exists**

Run:

~~~bash
.venv/bin/python -m pytest test/test_log_store.py -q
~~~

Expected: the new assertions fail because initialize() currently has no legacy import or retention implementation.

- [ ] **Step 3: Implement streamed, idempotent legacy migration**

Implement migrate_legacy() called by initialize() after schema creation. Read the JSONL file line by line, never call read_text() or load the whole file, and batch inserts (for example, 500 rows per transaction). For each line:

~~~python
try:
    payload = json.loads(line)
except json.JSONDecodeError:
    invalid_lines += 1
    continue
if not isinstance(payload, dict):
    invalid_lines += 1
    continue
~~~

Normalize missing fields to the current public shape, preserve a valid legacy ID, and derive a stable SHA-256-based ID from the line number and raw line when the ID is missing. Parse a valid legacy time into created_ts; use the file mtime for an invalid or missing legacy time so imported records remain grouped with their source. Use INSERT OR IGNORE to make retries safe.

Write a JSON migration record into log_metadata containing source path, imported count, invalid count, skipped duplicate count, completion time, and backup path. Commit all valid rows and metadata before renaming the source to logs.jsonl.migrated; if that name exists, append a timestamp suffix. If import or commit raises, leave the original JSONL untouched and propagate the startup error. If metadata already says the source was completed, skip migration. An empty legacy file is still considered successfully migrated and should be renamed.

- [ ] **Step 4: Implement retention cleanup and backup-name handling**

Add cleanup_expired(retention_days) that validates a positive integer, computes time.time() - retention_days * 86400, and deletes old rows in a short transaction. Keep the operation idempotent and return the deleted count. Add a private backup-path helper that never overwrites an existing .migrated file.

- [ ] **Step 5: Run the migration and retention tests, then commit**

Run:

~~~bash
.venv/bin/python -m pytest test/test_log_store.py -q
~~~

Expected: all storage, migration, idempotency, backup naming, failure-preservation, and retention tests pass.

Commit:

~~~bash
git add services/log_store.py test/test_log_store.py
git commit -m "feat: migrate and retain sqlite logs"
~~~

### Task 4: Keep LogService compatible while reducing persisted payloads

**Files:**
- Modify: services/log_service.py
- Modify: test/test_log_service.py
- Reference: services/log_store.py

- [ ] **Step 1: Add failing facade and payload-policy tests**

Extend the existing tests to prove that LogService delegates list/add/delete operations to SQLite and that LoggedCall.log() does not persist request text or response payloads:

~~~python
def test_logged_call_keeps_only_safe_metadata(self):
    call = LoggedCall(
        "secret prompt that must not be stored",
        {"model": "test", "stream": False},
        account="acct-1",
        conversation_id="conv-1",
    )
    with mock.patch("services.log_service.log_service.add") as add:
        call.log("调用完成", {"status": 200, "request_text": "secret", "data": "large"})
    detail = add.call_args.args[2]
    self.assertNotIn("request_text", detail)
    self.assertNotIn("data", detail)
    self.assertEqual(detail["request_shape"]["model"], "test")
~~~

Add an integration test with a temporary LogService that calls initialize(), writes a row, reads it with paginated query(), and deletes it. Keep a compatibility test for list(type_filter, start_date, end_date, limit) because existing callers may still use that method.

- [ ] **Step 2: Run the new tests and verify they fail against the JSONL implementation**

Run:

~~~bash
.venv/bin/python -m pytest test/test_log_service.py -q
~~~

Expected: the SQLite integration test cannot construct/use the new facade, and the payload assertion fails because the current implementation stores the request excerpt.

- [ ] **Step 3: Replace the JSONL facade with the SQLite-backed facade**

Keep the public LogService.add, list, and delete names for compatibility, but route them through a SQLiteLogStore. Add:

~~~python
def initialize(self) -> None: ...
def query(self, *, type_filter="", start_date="", end_date="", page=1, page_size=20) -> dict: ...
def cleanup_expired(self, retention_days: int) -> int: ...
def start_retention_scheduler(self, stop_event, retention_days_getter) -> threading.Thread: ...
~~~

Use a lock/flag so initialization is performed once. The global instance must point to DATA_DIR / "logs.db" with DATA_DIR / "logs.jsonl" as the migration source. list() should be a thin compatibility wrapper that returns the first page's items with the old limit behavior.

Retain LoggedCall.request_text in memory for call-site compatibility, but remove _request_excerpt() from the persisted detail. LoggedCall.log() may persist only bounded metadata: account, conversation_id, request_shape, error, and safe generated URLs where the UI requires them. Explicitly omit request_text, prompt content, base64 data, response bodies, and generated binary content. Keep one terminal call record per request.

- [ ] **Step 4: Run the focused service tests and commit**

Run:

~~~bash
.venv/bin/python -m pytest test/test_log_service.py test/test_log_store.py -q
~~~

Expected: all facade compatibility and payload-policy tests pass, and no test reads or writes logs.jsonl through normal runtime operations.

Commit:

~~~bash
git add services/log_service.py test/test_log_service.py
git commit -m "refactor: route application logs through sqlite"
~~~

### Task 5: Add retention configuration and startup scheduling

**Files:**
- Modify: services/config.py
- Modify: config.json
- Modify: api/app.py
- Create or modify: test/test_config.py or the existing config test module
- Modify: test/test_log_store.py if scheduler tests are colocated there

- [ ] **Step 1: Add failing configuration and lifecycle tests**

Test that a missing setting resolves to 30, configured values are normalized to a positive integer, and the value is included in ConfigStore.get()/update() round trips. Add a scheduler test using a threading.Event and a fake cleanup callback to prove the worker exits after the event is set; do not wait a real day in the test, inject the interval or patch the wait function.

- [ ] **Step 2: Run the tests and verify they fail because the setting and scheduler do not exist**

Run:

~~~bash
.venv/bin/python -m pytest test/test_config.py test/test_log_store.py -q
~~~

Expected: the config property and scheduler test fail against the current implementation.

- [ ] **Step 3: Add the 30-day setting**

Add log_retention_days near the existing retention properties in ConfigStore. Normalize invalid/non-positive values to 30 or the minimum accepted positive value according to the existing config conventions. Include the setting in the normalized get() result and add the default key to config.json without changing unrelated user-authored values, especially the already committed auth configuration.

- [ ] **Step 4: Wire initialization, startup cleanup, and daily cleanup into lifespan**

In api/app.py, initialize the log service before starting account/image background workers. Run startup initialization and the first retention cleanup before yielding the application. Start the retention thread with a stop event and a getter for the current config value. Stop and join it during shutdown. Initialization failures must abort startup; cleanup failures should be emitted to the runtime logger without taking the service down.

The lifecycle sequence must be equivalent to:

~~~python
await run_in_threadpool(log_service.initialize)
await run_in_threadpool(log_service.cleanup_expired, config.log_retention_days)
log_cleanup_thread = log_service.start_retention_scheduler(
    stop_event,
    lambda: config.log_retention_days,
)
~~~

Use the existing stop_event so shutdown remains coordinated with the other schedulers.

- [ ] **Step 5: Run config/lifecycle tests and commit**

Run:

~~~bash
.venv/bin/python -m pytest test/test_config.py test/test_log_store.py -q
~~~

Expected: config defaults, normalization, scheduler shutdown, and existing config tests pass.

Commit:

~~~bash
git add services/config.py config.json api/app.py test/test_config.py test/test_log_store.py
git commit -m "feat: configure and schedule log retention"
~~~

### Task 6: Stop routine account refreshes from flooding the log store

**Files:**
- Modify: services/account_service.py
- Modify: api/support.py
- Modify: api/accounts.py
- Modify: account-service tests covering refresh/update behavior

- [ ] **Step 1: Add failing tests for quiet periodic refresh and explicit summaries**

Patch log_service.add and exercise refresh_accounts with a successful fake remote response. Assert that a periodic refresh emits no successful per-account 更新账号 record, while token rotations and refresh failures still emit their required records. Add a separate explicit-refresh test that requests an aggregate summary and asserts one summary record with counts rather than one success record per account.

- [ ] **Step 2: Run the focused account tests and confirm the current noise remains**

Run:

~~~bash
.venv/bin/python -m pytest test/test_account_service.py -q
~~~

If the repository uses a different account test module, locate it with rg -n "refresh_accounts|更新账号|fetch_remote_info" test. Expected: the new quiet-refresh assertion fails because fetch_remote_info() currently calls update_account() with quiet=False.

- [ ] **Step 3: Add keyword-only quiet and summary controls without breaking positional callers**

Extend the existing signatures after the current positional parameters:

~~~python
def fetch_remote_info(..., defer_invalid_removal=True, *, quiet=False): ...
def refresh_accounts(..., defer_invalid_removal=True, *, quiet=False, log_summary=False): ...
~~~

Pass quiet through the executor to fetch_remote_info() and then to update_account(..., quiet=quiet). Preserve token rotation, invalid-token removal, and error logging even when quiet=True. When log_summary=True, emit one bounded aggregate record containing requested, refreshed, failed, and removed counts; do not include token values, prompts, or full remote responses.

- [ ] **Step 4: Update all call sites with an explicit policy**

Use the following policy:

~~~python
# api/support.py: automatic five-minute watcher
account_service.refresh_accounts(tokens, quiet=True)

# api/accounts.py: explicit admin refresh
account_service.refresh_accounts(
    access_tokens,
    progress_id,
    False,
    quiet=True,
    log_summary=True,
)

# internal access-token metadata refresh
fetch_remote_info(access_token, "get_available_access_token", quiet=True)
~~~

For account creation, keep the existing aggregate 新增账号 record and run the metadata refresh quietly; do not create a duplicate per-account success record. Keep manual account updates, imports, deletes, login/re-login, token rotation, invalid-token removal, and failures logged as specified in the design.

- [ ] **Step 5: Run focused and regression tests, then commit**

Run:

~~~bash
.venv/bin/python -m pytest test/test_account_service.py test/test_log_service.py -q
~~~

Expected: periodic success refreshes are silent, necessary failure/rotation logs remain, explicit refresh produces one summary, and existing account behavior is unchanged.

Commit:

~~~bash
git add services/account_service.py api/support.py api/accounts.py test
git commit -m "perf: reduce routine account refresh logs"
~~~

### Task 7: Make the logs API paginated and non-blocking

**Files:**
- Modify: api/system.py
- Create or modify: test/test_system_api.py or the existing system API test module

- [ ] **Step 1: Add failing API tests for pagination, validation, and threadpool delegation**

Test an authorized GET /api/logs with type, date range, page, and page_size query parameters. Assert the response shape is the store page object and that the service is invoked with the same filters. Test page < 1, page_size < 1, and page_size > 100 as HTTP 400 responses. Test POST /api/logs/delete delegates IDs and returns the deleted count.

The test should patch api.system.log_service.query/delete and, where needed, patch api.system.run_in_threadpool to record that the synchronous store call is not executed directly inside the async route.

- [ ] **Step 2: Run the API tests and confirm the old list response/validation fails**

Run:

~~~bash
.venv/bin/python -m pytest test/test_system_api.py -q
~~~

Expected: the current endpoint returns only items, does not accept server pagination, and calls the synchronous service directly.

- [ ] **Step 3: Implement validated query parameters and threadpool access**

Update GET /api/logs to accept type, start_date, end_date, page=1, and page_size=20. Validate date strings and numeric bounds, raising HTTPException(status_code=400, ...) for invalid values. Call log_service.query through await run_in_threadpool(...) and return its complete page object.

Update POST /api/logs/delete to validate that ids is a list of strings, execute log_service.delete through run_in_threadpool, and return a compact result such as {"deleted": count}. Preserve admin authorization before any database work.

- [ ] **Step 4: Run API and broader backend tests, then commit**

Run:

~~~bash
.venv/bin/python -m pytest test/test_system_api.py test/test_log_service.py test/test_log_store.py -q
~~~

Expected: pagination, validation, authorization, deletion, and existing system routes pass.

Commit:

~~~bash
git add api/system.py test/test_system_api.py
git commit -m "perf: paginate log api off the event loop"
~~~

### Task 8: Move the admin log page to server-side pagination

**Files:**
- Modify: web/src/lib/api.ts
- Modify: web/src/app/logs/page.tsx

- [ ] **Step 1: Update the API client types and request parameters**

Change fetchSystemLogs to accept page and page_size in addition to the existing type/date filters and return:

~~~ts
type SystemLogsResponse = {
  items: SystemLog[];
  total: number;
  page: number;
  page_size: number;
  has_more: boolean;
};
~~~

Use URLSearchParams to omit empty filters and request only the current page.

- [ ] **Step 2: Replace client slicing with server pagination**

In the logs page, keep the existing type/date filters, detail modal, delete action, and current-page selection. Use a stable page size of 20, track page, total, and hasMore, reset the page to 1 when filters change, and render the API's items directly. Display the server total rather than the number of loaded rows.

Remove the 全选结果/all-loaded-results control because the browser no longer owns the full result set. Keep 本页全选 scoped to the loaded page. After deletion, reload the current page and move to the previous page if the current page becomes empty.

- [ ] **Step 3: Build the frontend and commit**

Run:

~~~bash
cd web
bun run build
~~~

Expected: the production build completes with no TypeScript errors. Then commit:

~~~bash
cd ..
git add web/src/lib/api.ts web/src/app/logs/page.tsx
git commit -m "feat: paginate admin log view"
~~~

### Task 9: Document operations and run full verification

**Files:**
- Modify: docs/deployment.md or the repository's existing deployment documentation
- Modify: README.md only if it is the canonical operations guide
- Modify: docs/superpowers/specs/2026-09-07-sqlite-log-system-design.md only if implementation details need a recorded correction

- [ ] **Step 1: Document the runtime behavior**

Document that runtime logs still go to the process logger, admin/business logs are stored in data/logs.db, the first startup migrates data/logs.jsonl and preserves a .migrated backup, and log_retention_days controls cleanup with a 30-day default. Include the SQLite file in backup/volume guidance and explain that the admin API is paginated.

- [ ] **Step 2: Run formatting and static checks**

Run:

~~~bash
.venv/bin/python -m compileall api services test
git diff --check main...HEAD
~~~

Run the repository's existing lint/type commands if they are defined in pyproject.toml or package scripts; do not introduce a new formatter configuration for this change.

- [ ] **Step 3: Run the complete backend test suite**

Run:

~~~bash
.venv/bin/python -m pytest -q
~~~

Expected: the complete existing suite and all new log-system tests pass. Pay particular attention to account refresh, system API, config, and image-related tests because they share the application lifespan and logging facade.

- [ ] **Step 4: Verify the migration and query path with a temporary production-shaped data directory**

Use a temporary directory containing a small legacy logs.jsonl, start the service's log initialization path, and verify:

~~~text
data/logs.db exists
valid legacy rows are queryable
logs.jsonl is renamed to logs.jsonl.migrated (or a timestamped variant)
re-running initialization does not duplicate rows
retention cleanup removes only rows older than the configured cutoff
~~~

Do not modify the repository's real data/ files during this check.

- [ ] **Step 5: Review the final diff and working tree**

Run:

~~~bash
git status --short
git diff --stat main...HEAD
git log --oneline --decorate -12
~~~

Confirm that the feature branch contains only the approved SQLite log refactor, the config default, tests, and documentation; no real database, migrated backup, secrets, or generated frontend output is committed.
