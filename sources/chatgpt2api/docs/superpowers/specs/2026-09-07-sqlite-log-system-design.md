# SQLite Log System Refactor Design

**Status:** Approved

## Goal

Replace the append-only JSONL business log store with an indexed SQLite log store that keeps the log management page responsive as the service grows, reduces high-volume routine logs, migrates existing records safely, and removes request-body persistence from new call logs.

## Confirmed Decisions

- The business log database is a separate SQLite file at `data/logs.db`.
- Existing `data/logs.jsonl` records are migrated automatically on the first startup that initializes the new store.
- Migration is streamed and batched so the legacy file is never loaded into memory at once.
- After a successful migration, the legacy file is renamed to `logs.jsonl.migrated`; if that name already exists, a timestamped suffix is used.
- Migration is idempotent and preserves valid historical records, including their existing details.
- New logs are retained for 30 days by default.
- Retention is configurable through `log_retention_days` and cleanup runs at startup and periodically while the service is running.
- New call logs do not persist `request_text`.
- Routine successful account refreshes do not create one log entry per account.
- SQLite is used only for the log subsystem; the existing account storage backend remains independently configurable.
- The existing log management capabilities remain available: type/date filters, detail inspection, and deletion.

## Current Problems

The current `LogService` appends serialized records to `data/logs.jsonl`. Every query reads the complete file, splits it into an in-memory list, then parses records backwards until it finds enough matches. The API calls this synchronous method directly from an async route, so the read and JSON parsing block the single Uvicorn event loop.

The frontend receives up to 200 records and performs pagination in the browser. This reduces rendered rows but does not reduce server-side disk I/O, parsing, or response construction.

The account watcher runs every five minutes. It refreshes normal accounts and eventually calls `update_account` with the default non-quiet behavior, producing an `更新账号` entry for each successful refresh. This is routine state synchronization, not an auditable user action, and is the dominant source of log growth.

## Architecture

### Storage boundary

Introduce a focused SQLite-backed store behind the existing `log_service` module. The store owns:

- SQLite connection setup and pragmas.
- Schema creation and schema versioning.
- Single-record inserts and batched migration inserts.
- Filtered, ordered, paginated queries.
- Bulk deletion by log IDs.
- Retention cleanup.
- Legacy JSONL migration.

The public service surface remains small and independent of account storage:

```python
log_service.add(type, summary, detail)
log_service.query(type, start_date, end_date, page, page_size)
log_service.delete(ids)
log_service.cleanup_expired(retention_days)
log_service.initialize()
```

`query` returns both the current page and total count. A compatibility `list` wrapper remains for internal callers that only need a list, while API handlers use the paginated query method.

### SQLite configuration

The store uses the standard-library `sqlite3` module and one connection per thread. Connections are configured with:

- `journal_mode=WAL` for concurrent readers during writes.
- `synchronous=NORMAL` to avoid unnecessary fsync cost while retaining transactional durability suitable for operational logs.
- `busy_timeout=5000` to tolerate short writer contention.
- `foreign_keys=ON` for future schema safety.

Writes are short transactions. Migration uses explicit batches. Deletion and retention cleanup use one transaction per operation and never rewrite the complete log table.

### Data flow

```text
request/account event
        |
        v
log_service.add()
        |
        v
SQLite INSERT --------------------+
        |                          |
        v                          v
indexed query <------------- log management API
        |
        v
server-paginated frontend
```

## Database Schema

The initial schema is:

```sql
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
    ON logs (created_ts DESC, sequence DESC);

CREATE INDEX IF NOT EXISTS idx_logs_type_time
    ON logs (type, created_ts DESC, sequence DESC);
```

`sequence` provides deterministic insertion order when multiple events share the same timestamp. `id` remains the public identifier used by the frontend and delete API. `detail_json` stores the existing structured detail object without creating a wide, unstable schema for every event type.

Date filters use the existing local `YYYY-MM-DD` input and are converted to inclusive start and exclusive next-day boundaries. Queries order by `created_ts DESC, sequence DESC`.

## Logging Policy

### Call logs

Each externally visible API operation gets at most one terminal log entry:

- successful non-stream response: one success entry;
- successful stream: one completion entry;
- failed operation: one failure entry;
- image task: one terminal completion or failure entry.

The persisted detail contains only operational metadata: API key identity, endpoint, model, start/end time, duration, status, account email when available, conversation ID when available, safe external URLs when needed for the image UI, request shape, and a bounded error message. `request_text`, base64 data, response payloads, and generated image bytes are not persisted.

### Account logs

Keep logs for events that explain an account state change or a user action:

- account import, deletion, manual update, login, and relogin;
- refresh-token rotation;
- refresh failures, invalid tokens, account disablement, or automatic removal;
- one aggregate entry for an explicit bulk refresh operation when it is initiated by an administrator.

Do not log successful periodic account refreshes. The watcher passes quiet behavior through the refresh path, while failures and token rotation continue to be recorded.

## API Contract

`GET /api/logs` accepts:

```text
type       optional log type
start_date optional YYYY-MM-DD
end_date   optional YYYY-MM-DD
page       positive integer, default 1
page_size  positive integer, default 20, maximum 100
```

The response is:

```json
{
  "items": [],
  "total": 0,
  "page": 1,
  "page_size": 20,
  "has_more": false
}
```

The route executes database work in a threadpool so synchronous SQLite I/O cannot block the event loop. Invalid dates or non-positive pagination values return a clear `400` response. The query uses indexed predicates and `LIMIT/OFFSET`; the 30-day retention window keeps offset depth bounded for the intended workload.

`POST /api/logs/delete` deletes the supplied IDs in one SQLite transaction and returns the number removed. It does not read or rewrite unrelated log rows.

## Frontend Behavior

The logs page requests one server page at a time and uses the returned `total`, `page`, `page_size`, and `has_more` values for navigation. Filter changes reset to page one. The detail dialog and image URL display remain unchanged.

Selection is limited to records currently loaded in the browser. The old “select all results” control is removed so the UI does not claim to select records that were not loaded; deletion still sends selected IDs in one request.

## Migration and Retention

Initialization creates the database schema before serving requests. If `data/logs.jsonl` exists and migration has not completed, the migrator:

1. reads one line at a time;
2. skips malformed or non-object records while counting them;
3. preserves existing IDs and derives deterministic IDs for legacy records without IDs;
4. inserts records in bounded batches using `INSERT OR IGNORE` for restart safety;
5. commits the final batch;
6. renames the source file only after all valid rows are committed;
7. records migration counts, source metadata, and the backup name in a dedicated metadata table.

If the process stops during migration, the next startup can safely rerun it because IDs are unique and inserts are idempotent. The original file is never deleted.

Retention deletes records older than the configured number of days in a bounded transaction. Cleanup runs once during startup and from a daemon scheduler no more than once per day. Cleanup failures are reported to the runtime logger but do not prevent the API from starting.

## Error Handling and Concurrency

- A failed log write must not turn a successful upstream API request into a failure; the existing call paths retain their current best-effort logging behavior.
- Database initialization failures are fatal at startup because serving without a usable log store would make the admin log page misleading.
- SQLite write contention is handled with the configured busy timeout and short transactions.
- Legacy migration errors are isolated per line; one malformed historical record does not abort valid-record migration.
- Deletion and retention use transactions, so readers see either the previous or the committed state.
- The database file is stored under the existing mounted `data/` directory and is covered by the existing `*.db` ignore rule.

## Testing and Acceptance Criteria

Add focused tests before implementation for:

- insert and read round trips with structured details;
- newest-first ordering when timestamps are equal;
- type/date filtering and bounded pagination;
- bulk deletion without rewriting unrelated rows;
- retention cleanup at the exact cutoff;
- migration of valid, malformed, and ID-less JSONL rows;
- migration restart/idempotency and legacy-file backup naming;
- concurrent writes from multiple threads;
- call logs excluding `request_text` while retaining operational metadata;
- periodic account refreshes not emitting successful per-account logs;
- API pagination and validation behavior.

The refactor is accepted when:

- log queries do not read or parse the legacy file;
- the response size is bounded by `page_size`;
- the event loop is not blocked by log database operations;
- routine successful account refreshes no longer create per-account records;
- existing valid history is available in SQLite after migration;
- logs older than the configured retention period are removed;
- existing log detail, filtering, and deletion workflows continue to work.

## Out of Scope

- Replacing Python's runtime stdout logger with a distributed logging platform.
- Adding Elasticsearch, Kafka, or an external log service.
- Changing account storage backends.
- Persisting full request/response payloads for debugging.
