# Unified SQLite State Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move accounts, user keys, tasks, scheduler state, and quota state into one normalized `app.db` without changing public behavior or losing existing data.

**Architecture:** Add a stdlib `sqlite3` state backend that owns normalized account and auth-key rows and exposes incremental upsert/delete operations. Reuse the same SQLite file for the scheduler and quota stores, and add an idempotent migration coordinator that imports the current JSON and legacy SQLite sources in one transaction while leaving source files intact.

**Tech Stack:** Python 3.13, FastAPI, stdlib `sqlite3`, existing scheduler/quota services, pytest, `uv`.

---

## File map

- Create `services/storage/sqlite_state.py`: connection configuration, normalized schema, account/auth-key row mapping, incremental storage operations, and lifecycle cleanup.
- Create `services/storage/sqlite_migration.py`: legacy JSON/SQLite import, source verification, migration marker, and rollback-safe transaction.
- Create `scripts/migrate_to_sqlite.py`: explicit migration/verification CLI for deployment and rollback preparation.
- Create `test/test_sqlite_state.py`: schema, row mapping, incremental update, auth-key, and lifecycle tests.
- Create `test/test_sqlite_migration.py`: idempotent import, count/key verification, failure rollback, and legacy-file preservation tests.
- Create `test/test_storage_factory.py`: default backend and shared database path tests.
- Create `test/test_account_service.py`: incremental account-write and progress-summary tests.
- Create `test/test_auth_service.py`: incremental user-key write and Web login permission tests.
- Create `test/test_editable_file_task_service.py`: shared task persistence compatibility tests.
- Modify `services/storage/base.py`: add optional incremental methods with compatibility defaults so JSON/Git backends remain valid.
- Modify `services/storage/factory.py`: lazy-load optional backends and select the normalized SQLite backend by default.
- Modify `services/config.py`: expose `app.db` and migration/storage settings without changing admin-key configuration.
- Modify `services/account_service.py`: use incremental storage methods and persist token aliases/limits without a full JSON snapshot on hot updates.
- Modify `services/auth_service.py`: use incremental user-key writes while keeping the in-memory auth index for request-time authentication.
- Modify `services/scheduler/state_store.py`: accept the shared app database, preserve the existing scheduler API, and query tasks by indexed state.
- Modify `services/scheduler/service.py`: default the state store to `data/app.db` and close it during shutdown.
- Modify `services/quota_service.py`: default quota state to `data/app.db` and retain atomic reservation semantics.
- Modify `services/image_task_service.py`: use bounded task queries/retention when the shared store is active.
- Modify `services/editable_file_task_service.py`: persist task metadata through the shared task store when running in production.
- Modify `api/app.py`: run migration before state services initialize and close shared stores during lifespan shutdown.
- Modify `test/test_scheduler_state_store.py`, `test/test_scheduler_image_integration.py`, `test/test_image_task_service.py`, and quota tests: assert the shared-path behavior while retaining explicit temporary database paths.

### Task 1: Define the normalized SQLite state schema

**Files:**
- Create: `services/storage/sqlite_state.py`
- Create: `test/test_sqlite_state.py`

- [ ] **Step 1: Write the failing schema and connection tests**

```python
def test_initialize_creates_normalized_state_tables(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)

    store.initialize()

    tables = store.table_names()
    assert {
        "schema_migrations",
        "accounts",
        "account_limits",
        "account_token_aliases",
        "auth_keys",
        "tasks",
        "key_quotas",
        "quota_reservations",
        "scheduler_account_health",
    } <= tables


def test_connection_uses_wal_and_bounded_cache(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    store.initialize()

    assert store.scalar("PRAGMA journal_mode") == "wal"
    assert store.scalar("PRAGMA busy_timeout") == 5000
    assert store.scalar("PRAGMA cache_size") < 0
```

- [ ] **Step 2: Run the focused tests and verify the missing implementation fails**

Run: `uv run pytest test/test_sqlite_state.py -q`

Expected: collection fails with `ModuleNotFoundError` for `services.storage.sqlite_state`.

- [ ] **Step 3: Implement the schema and connection factory**

```python
class SQLiteStateStore:
    CACHE_SIZE_KIB = -4096

    def __init__(self, db_path: Path, *, auto_migrate: bool = True):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.auto_migrate = auto_migrate
        self._local = threading.local()
        self._lock = threading.RLock()
        self._initialized = False

    def _connection(self) -> sqlite3.Connection:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(str(self.db_path), timeout=5.0)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute(f"PRAGMA cache_size={self.CACHE_SIZE_KIB}")
            connection.execute("PRAGMA foreign_keys=ON")
            self._local.connection = connection
        return connection

    def initialize(self) -> None:
        with self._lock:
            if self._initialized:
                return
            self._connection().executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    source_fingerprint TEXT NOT NULL DEFAULT '',
                    verification_json TEXT NOT NULL DEFAULT '{}',
                    completed_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    access_token TEXT NOT NULL UNIQUE,
                    refresh_token TEXT NOT NULL DEFAULT '',
                    id_token TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '',
                    user_id TEXT NOT NULL DEFAULT '',
                    account_type TEXT NOT NULL DEFAULT '',
                    source_type TEXT NOT NULL DEFAULT '',
                    export_type TEXT NOT NULL DEFAULT '',
                    proxy TEXT NOT NULL DEFAULT '',
                    password TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    token_generation INTEGER NOT NULL DEFAULT 0,
                    invalid_count INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    fail_count INTEGER NOT NULL DEFAULT 0,
                    default_model_slug TEXT NOT NULL DEFAULT '',
                    last_used_at REAL,
                    last_invalid_at REAL,
                    last_refresh_error TEXT NOT NULL DEFAULT '',
                    last_refresh_error_at REAL,
                    last_token_refresh_at REAL,
                    last_token_refresh_error TEXT NOT NULL DEFAULT '',
                    created_at REAL,
                    updated_at REAL NOT NULL,
                    extra_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
                CREATE INDEX IF NOT EXISTS idx_accounts_email ON accounts(email);
                CREATE TABLE IF NOT EXISTS account_limits (
                    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                    capability TEXT NOT NULL,
                    remaining INTEGER,
                    reset_at REAL,
                    raw_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY(account_id, capability)
                );
                CREATE TABLE IF NOT EXISTS account_token_aliases (
                    alias_token TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                    current_token TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    last_used_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_token_alias_expiry
                    ON account_token_aliases(expires_at);
                CREATE TABLE IF NOT EXISTS auth_keys (
                    key_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    key_hash TEXT NOT NULL UNIQUE,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    web_login_enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_used_at TEXT,
                    extra_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_auth_keys_role_enabled
                    ON auth_keys(role, enabled);
                CREATE TABLE IF NOT EXISTS tasks (
                    task_key TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL DEFAULT '',
                    task_id TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    model TEXT NOT NULL DEFAULT '',
                    remote_task_id TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT,
                    error TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_owner_updated
                    ON tasks(owner_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_tasks_status_updated
                    ON tasks(status, updated_at DESC);
                CREATE TABLE IF NOT EXISTS key_quotas (
                    key_id TEXT PRIMARY KEY,
                    text_limit INTEGER,
                    image_limit INTEGER,
                    text_used INTEGER NOT NULL DEFAULT 0,
                    image_used INTEGER NOT NULL DEFAULT 0,
                    text_reserved INTEGER NOT NULL DEFAULT 0,
                    image_reserved INTEGER NOT NULL DEFAULT 0,
                    deleted_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS quota_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    key_id TEXT NOT NULL REFERENCES key_quotas(key_id) ON DELETE CASCADE,
                    text_units INTEGER NOT NULL DEFAULT 0,
                    image_units INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_quota_reservations_key_status
                    ON quota_reservations(key_id, status);
                CREATE INDEX IF NOT EXISTS idx_quota_reservations_expiry
                    ON quota_reservations(expires_at, status);
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
                    PRIMARY KEY(account_key, lane)
                );
                """
            )
            self._connection().commit()
            self._initialized = True
```

The implementation must use parameterized SQL, `ON DELETE CASCADE`, and `extra_json` only for fields outside the explicit hot-field allowlist. It must not put the complete input account dictionary into one JSON column.

- [ ] **Step 4: Run the focused tests and verify the schema passes**

Run: `uv run pytest test/test_sqlite_state.py::test_initialize_creates_normalized_state_tables test/test_sqlite_state.py::test_connection_uses_wal_and_bounded_cache -q`

Expected: both tests pass.

- [ ] **Step 5: Commit the schema foundation**

```bash
git add services/storage/sqlite_state.py test/test_sqlite_state.py
git commit -m "feat: add normalized sqlite state schema"
```

### Task 2: Implement normalized account and user-key storage

**Files:**
- Modify: `services/storage/sqlite_state.py`
- Modify: `services/storage/base.py`
- Test: `test/test_sqlite_state.py`

- [ ] **Step 1: Write round-trip and incremental-update tests**

```python
def test_account_round_trip_restores_hot_fields_limits_and_unknown_fields(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    store.initialize()
    account = {
        "access_token": "token-a",
        "refresh_token": "refresh-a",
        "email": "a@example.com",
        "status": "active",
        "limits_progress": [{"name": "text", "remaining": 42, "reset_after": 12}],
        "proxy": "http://proxy.local",
        "custom_flag": "kept",
    }

    store.save_account(account)
    loaded = store.load_accounts()

    assert loaded == [account]


def test_save_account_updates_one_row_without_snapshot_delete(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    store.initialize()
    store.save_accounts([
        {"access_token": "token-a", "email": "a@example.com"},
        {"access_token": "token-b", "email": "b@example.com"},
    ])

    store.save_account({"access_token": "token-a", "email": "new@example.com"})

    assert {row["access_token"] for row in store.load_accounts()} == {"token-a", "token-b"}
    assert store.load_accounts()[0]["email"] == "new@example.com"


def test_user_key_round_trip_preserves_web_login_permission(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    store.initialize()
    item = {
        "id": "user-1",
        "name": "client",
        "role": "user",
        "key_hash": "hash-1",
        "enabled": True,
        "web_login_enabled": False,
        "created_at": "2026-09-09T00:00:00+00:00",
        "last_used_at": None,
    }

    store.save_auth_key(item)

    assert store.load_auth_keys() == [item]
```

- [ ] **Step 2: Run the tests and verify the new methods fail**

Run: `uv run pytest test/test_sqlite_state.py -q`

Expected: FAIL because account/auth-key row mapping and incremental methods are not implemented.

- [ ] **Step 3: Add compatibility methods to the storage contract**

```python
class StorageBackend(ABC):
    def save_account(self, account: dict[str, Any]) -> None:
        self.save_accounts([account])

    def delete_account(self, access_token: str) -> None:
        accounts = [
            item for item in self.load_accounts()
            if str(item.get("access_token") or "") != str(access_token or "")
        ]
        self.save_accounts(accounts)

    def save_auth_key(self, auth_key: dict[str, Any]) -> None:
        items = self.load_auth_keys()
        key_id = str(auth_key.get("id") or "")
        items = [item for item in items if str(item.get("id") or "") != key_id]
        items.append(auth_key)
        self.save_auth_keys(items)

    def delete_auth_key(self, key_id: str) -> None:
        self.save_auth_keys([
            item for item in self.load_auth_keys()
            if str(item.get("id") or "") != str(key_id or "")
        ])
```

The defaults preserve JSON/Git behavior. `SQLiteStateStore` overrides them with single-row SQL writes.

- [ ] **Step 4: Implement explicit-column mapping and child-row replacement**

```python
def save_account(self, account: dict[str, Any]) -> None:
    normalized = self._normalize_account(account)
    with self.transaction() as connection:
        connection.execute(self._ACCOUNT_UPSERT_SQL, normalized.hot_values)
        connection.execute(
            "DELETE FROM account_limits WHERE account_id = ?",
            (normalized.account_id,),
        )
        connection.executemany(
            """INSERT INTO account_limits
               (account_id, capability, remaining, reset_at, raw_json)
               VALUES (?, ?, ?, ?, ?)""",
            normalized.limit_rows,
        )

def save_auth_key(self, auth_key: dict[str, Any]) -> None:
    normalized = self._normalize_auth_key(auth_key)
    with self.transaction() as connection:
        connection.execute(
            """INSERT INTO auth_keys
               (key_id, name, role, key_hash, enabled, web_login_enabled,
                created_at, last_used_at, extra_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(key_id) DO UPDATE SET
                 name=excluded.name, role=excluded.role,
                 key_hash=excluded.key_hash, enabled=excluded.enabled,
                 web_login_enabled=excluded.web_login_enabled,
                 created_at=excluded.created_at,
                 last_used_at=excluded.last_used_at,
                 extra_json=excluded.extra_json""",
            normalized,
        )
```

`load_accounts()` and `load_auth_keys()` may reconstruct compatibility dictionaries for startup and admin pages, but hot updates must call `save_account()`/`save_auth_key()` and must not read or rewrite `accounts.json`/`auth_keys.json`.

- [ ] **Step 5: Run all state tests and the existing storage tests**

Run: `uv run pytest test/test_sqlite_state.py test/test_database_storage.py test/test_account_image_capabilities.py -q`

Expected: all tests pass; JSON and legacy SQLAlchemy backend tests remain unchanged.

- [ ] **Step 6: Commit normalized account/key storage**

```bash
git add services/storage/base.py services/storage/sqlite_state.py test/test_sqlite_state.py
git commit -m "feat: persist accounts and auth keys in normalized sqlite rows"
```

### Task 3: Add idempotent legacy migration and verification CLI

**Files:**
- Create: `services/storage/sqlite_migration.py`
- Create: `scripts/migrate_to_sqlite.py`
- Create: `test/test_sqlite_migration.py`

- [ ] **Step 1: Write migration fixture tests**

```python
def test_migration_imports_json_and_legacy_sqlite_once(tmp_path):
    write_json(tmp_path / "accounts.json", [{"access_token": "a", "email": "a@example.com"}])
    write_json(tmp_path / "auth_keys.json", {"items": [
        {"id": "u1", "role": "user", "name": "one", "key_hash": "h1",
         "enabled": True, "web_login_enabled": False,
         "created_at": "2026-09-09T00:00:00+00:00", "last_used_at": None}
    ]})
    create_legacy_scheduler_db(tmp_path / "scheduler.db", task_key="task-1")
    create_legacy_quota_db(tmp_path / "auth_quota.db", key_id="u1")

    first = migrate_legacy_state(tmp_path, tmp_path / "app.db")
    second = migrate_legacy_state(tmp_path, tmp_path / "app.db")

    assert first["accounts"] == 1
    assert first["auth_keys"] == 1
    assert second["already_migrated"] is True
    assert count_rows(tmp_path / "app.db", "accounts") == 1
    assert count_rows(tmp_path / "app.db", "tasks") == 1
    assert (tmp_path / "accounts.json").exists()
    assert (tmp_path / "scheduler.db").exists()


def test_migration_rolls_back_when_source_validation_fails(tmp_path, monkeypatch):
    write_json(tmp_path / "accounts.json", [{"access_token": "a"}])
    monkeypatch.setattr(
        "services.storage.sqlite_migration._validate_imported_counts",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("count mismatch")),
    )

    with pytest.raises(ValueError, match="count mismatch"):
        migrate_legacy_state(tmp_path, tmp_path / "app.db")

    assert count_rows(tmp_path / "app.db", "accounts") == 0
    assert not has_migration_marker(tmp_path / "app.db", 1)
```

- [ ] **Step 2: Run migration tests and verify the coordinator is missing**

Run: `uv run pytest test/test_sqlite_migration.py -q`

Expected: collection fails because `services.storage.sqlite_migration` does not exist.

- [ ] **Step 3: Implement one-transaction import with a version marker**

```python
def migrate_legacy_state(data_dir: Path, db_path: Path) -> dict[str, object]:
    sources = _discover_sources(Path(data_dir))
    connection = _connect_for_migration(Path(db_path))
    try:
        ensure_schema(connection)
        marker = connection.execute(
            "SELECT version FROM schema_migrations WHERE version = 1"
        ).fetchone()
        if marker is not None:
            return {"already_migrated": True}
        connection.execute("BEGIN IMMEDIATE")
        imported = _import_json_sources(connection, sources)
        imported.update(_import_legacy_sqlite_sources(connection, sources))
        _validate_imported_counts(connection, sources, imported)
        connection.execute(
            """INSERT INTO schema_migrations
               (version, source_fingerprint, verification_json, completed_at)
               VALUES (?, ?, ?, ?)""",
            (1, _fingerprint(sources), json.dumps(imported, sort_keys=True), time.time()),
        )
        connection.commit()
        return imported
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
```

Import rules are deterministic: account identity is the existing access token or a generated stable ID derived from the source index; user-key identity is the existing `id`; scheduler task identity is `task_key`; quota identity is the existing key/reservation ID. `INSERT OR IGNORE` is used only for legacy rows whose identity was already verified. Admin config is never read as a row and never imported.

- [ ] **Step 4: Implement the CLI with migrate and verify modes**

```python
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    db_path = args.db or args.data_dir / "app.db"
    result = verify_sqlite_state(db_path) if args.verify else migrate_legacy_state(args.data_dir, db_path)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run migration, repeat migration, and rollback tests**

Run: `uv run pytest test/test_sqlite_migration.py -q`

Expected: all migration tests pass, including source-file preservation and rollback.

Run: `uv run python scripts/migrate_to_sqlite.py --data-dir /tmp/chatgpt2api-fixture --verify`

Expected: JSON output contains table counts and `valid: true` for a migrated fixture.

- [ ] **Step 6: Commit migration tooling**

```bash
git add services/storage/sqlite_migration.py scripts/migrate_to_sqlite.py test/test_sqlite_migration.py
git commit -m "feat: add idempotent legacy state migration"
```

### Task 4: Switch production storage selection and shared database path

**Files:**
- Modify: `services/storage/factory.py`
- Modify: `services/config.py`
- Modify: `services/scheduler/service.py`
- Modify: `services/quota_service.py`
- Modify: `api/app.py`
- Test: `test/test_storage_factory.py`, `test/test_scheduler_state_store.py`, `test/test_quota_service.py`

- [ ] **Step 1: Add factory/config tests for the default shared path**

```python
def test_factory_defaults_to_normalized_sqlite(monkeypatch, tmp_path):
    monkeypatch.delenv("STORAGE_BACKEND", raising=False)

    backend = create_storage_backend(tmp_path)

    assert isinstance(backend, SQLiteStateStore)
    assert backend.db_path == tmp_path / "app.db"


def test_scheduler_and_quota_can_share_one_database(tmp_path):
    scheduler = SchedulerStateStore(tmp_path / "app.db")
    quota = AuthQuotaStore(tmp_path / "app.db")
    scheduler.initialize()
    quota.initialize()

    assert scheduler.db_path == quota.db_path == tmp_path / "app.db"
```

- [ ] **Step 2: Run the tests and verify default selection is still JSON**

Run: `uv run pytest test/test_storage_factory.py test/test_scheduler_state_store.py -q`

Expected: the new default-selection assertion fails because factory currently defaults to `json` and scheduler/quota use separate production defaults.

- [ ] **Step 3: Make SQLite the default and preserve explicit legacy backends**

```python
backend_type = os.getenv("STORAGE_BACKEND", "sqlite").lower().strip()

if backend_type == "sqlite":
    from services.storage.sqlite_state import SQLiteStateStore
    return SQLiteStateStore(data_dir / "app.db", legacy_data_dir=data_dir)

if backend_type == "json":
    from services.storage.json_storage import JSONStorageBackend
    return JSONStorageBackend(data_dir / "accounts.json", data_dir / "auth_keys.json")
```

Move SQLAlchemy, Git, and JSON imports into their individual branches. Add `ConfigStore.app_db_path` and `ConfigStore.storage_backend_name`, both with environment overrides. Change only the default scheduler/quota production constructors to `DATA_DIR / "app.db"`; explicit paths in tests and callers remain honored.

- [ ] **Step 4: Run storage, scheduler, quota, and startup tests**

Run: `uv run pytest test/test_storage_factory.py test/test_scheduler_state_store.py test/test_quota_service.py -q`

Expected: all tests pass and old explicit JSON/SQLite fixtures continue to work.

- [ ] **Step 5: Commit shared-path selection**

```bash
git add services/storage/factory.py services/config.py services/scheduler/service.py services/quota_service.py api/app.py test/test_storage_factory.py
git commit -m "feat: use shared sqlite app state database"
```

### Task 5: Replace hot full-snapshot writes in account and auth services

**Files:**
- Modify: `services/account_service.py`
- Modify: `services/auth_service.py`
- Modify: `services/storage/sqlite_state.py`
- Test: `test/test_sqlite_state.py`, `test/test_account_image_capabilities.py`, `test/test_account_service.py`, `test/test_auth_service.py`

- [ ] **Step 1: Write write-path tests**

```python
def test_account_service_update_uses_incremental_backend_method(tmp_path):
    storage = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    storage.initialize()
    service = AccountService(storage)
    service.add_accounts(["a"])

    service.update_account("a", {"email": "new@example.com"})

    assert storage.load_accounts()[0]["email"] == "new@example.com"
    assert not (tmp_path / "accounts.json").exists()


def test_auth_service_update_uses_single_key_upsert(tmp_path):
    storage = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    storage.initialize()
    service = AuthService(storage)
    public, raw_key = service.create_key(role="user", name="client")

    updated = service.update_key(public["id"], {"web_login_enabled": False})

    assert updated["web_login_enabled"] is False
    assert service.authenticate(raw_key, require_web_login=True) is None
```

- [ ] **Step 2: Run the tests and verify the hot-write hooks are absent**

Run: `uv run pytest test/test_account_image_capabilities.py -q`

Expected: the incremental storage test fails until service code calls the backend overrides.

- [ ] **Step 3: Route single-record mutations through optional backend methods**

```python
def _save_account(self, account: dict) -> None:
    saver = getattr(self.storage, "save_account", None)
    if callable(saver):
        saver(account)
        return
    self._save_accounts()

def _save(self) -> None:
    saver = getattr(self.storage, "save_auth_key", None)
    if callable(saver) and self._pending_single_item is not None:
        saver(self._pending_single_item)
        return
    self.storage.save_auth_keys(self._items)
```

Keep full snapshot writes for explicit bulk import/export and non-incremental backends. For SQLite, add transaction-aware methods for token alias replacement, account limits replacement, and key deletion so a token rotation does not leave child rows stale.

- [ ] **Step 4: Bound persisted alias and progress rows at the service boundary**

Persist only active token aliases with `expires_at > now`, and store progress result summaries (`total`, `completed`, `failed`, `error`, `finished_at`) rather than `list_accounts()` in the completed result. Existing API response fields remain available by querying account summaries when requested.

- [ ] **Step 5: Run account/auth regression tests**

Run: `uv run pytest test/test_account_image_capabilities.py test/test_account_service.py test/test_auth_service.py -q`

Expected: all account, authentication, Web login permission, token rotation, and JSON-backend compatibility tests pass.

- [ ] **Step 6: Commit incremental hot writes**

```bash
git add services/account_service.py services/auth_service.py services/storage/sqlite_state.py test/test_account_image_capabilities.py test/test_account_service.py test/test_auth_service.py
git commit -m "perf: avoid full state snapshots on account and key updates"
```

### Task 6: Unify task persistence and indexed recovery/retention

**Files:**
- Modify: `services/scheduler/state_store.py`
- Modify: `services/image_task_service.py`
- Modify: `services/editable_file_task_service.py`
- Modify: `api/app.py`
- Test: `test/test_scheduler_state_store.py`, `test/test_image_task_service.py`, `test/test_editable_file_task_service.py`

- [ ] **Step 1: Write shared-task and recovery-window tests**

```python
def test_shared_task_store_keeps_image_and_editable_metadata_in_one_table(tmp_path):
    store = SchedulerStateStore(tmp_path / "app.db")
    store.initialize()
    store.save_task({
        "task_key": "image:1", "owner_id": "u1", "task_id": "1",
        "mode": "image_generation", "status": "completed", "model": "gpt-image",
        "metadata": {"n": 1}, "result": {"url": "/images/1"}, "updated_at": 1,
    })
    store.save_task({
        "task_key": "file:1", "owner_id": "u1", "task_id": "1",
        "mode": "ppt", "status": "queued", "model": "", "metadata": {},
        "updated_at": 2,
    })

    assert {item["task_key"] for item in store.list_tasks(owner_id="u1")} == {"image:1", "file:1"}
    assert store.load_recoverable_tasks() == [store.get_task("file:1")]


def test_task_cleanup_is_indexed_and_does_not_load_old_rows(tmp_path):
    store = SchedulerStateStore(tmp_path / "app.db")
    store.initialize()
    store.save_task(completed_task("old", updated_at=1))

    removed = store.cleanup_tasks(now=100, retention_seconds=10, max_rows=100)

    assert removed == 1
    assert store.get_task("old") is None
```

- [ ] **Step 2: Run task tests and verify the current scheduler table/API lacks the shared behavior**

Run: `uv run pytest test/test_scheduler_state_store.py test/test_image_task_service.py test/test_editable_file_task_service.py -q`

Expected: the new shared-task tests fail before the table adapter and bounded list methods are added.

- [ ] **Step 3: Map the existing scheduler API onto the normalized `tasks` table**

Use `task_type` as the existing `mode` value, keep `metadata_json` filtered by the existing safe-key allowlist, and expose:

```python
def list_tasks(self, owner_id: str | None = None, *, limit: int = 1000) -> list[dict[str, object]]: ...
def load_recoverable_tasks(self, *, limit: int = 1000) -> list[dict[str, object]]: ...
def cleanup_tasks(self, *, now: float, retention_seconds: int, max_rows: int) -> int: ...
```

The SQL must filter `status` and `updated_at` before materializing rows. Create a compatibility view named `scheduler_tasks` for existing diagnostics; it selects normalized rows and does not duplicate task data.

- [ ] **Step 4: Route editable file task metadata through the same store**

Add an optional `task_store` parameter to `EditableFileTaskService`. When supplied, create/update/delete task rows through `save_task`/`delete_task`; retain its JSON path only for isolated legacy tests and explicit JSON rollback mode. Store file paths and result URLs, never Base64 payloads.

- [ ] **Step 5: Add startup cleanup and bounded recovery**

Call `cleanup_tasks` before loading recoverable tasks, with defaults of seven days and 10,000 rows. Load only non-terminal states plus recent task summaries. Do not call an unbounded `SELECT *` on startup.

- [ ] **Step 6: Run task integration tests**

Run: `uv run pytest test/test_scheduler_state_store.py test/test_scheduler_image_integration.py test/test_image_task_service.py test/test_editable_file_task_service.py -q`

Expected: all task persistence, quota reservation, restart recovery, owner filtering, and legacy JSON compatibility tests pass.

- [ ] **Step 7: Commit unified task storage**

```bash
git add services/scheduler/state_store.py services/image_task_service.py services/editable_file_task_service.py api/app.py test/test_scheduler_state_store.py test/test_scheduler_image_integration.py test/test_image_task_service.py test/test_editable_file_task_service.py
git commit -m "feat: unify task persistence and bounded recovery"
```

### Task 7: Complete verification and migration documentation

**Files:**
- Modify: `docs/deployment.md`
- Modify: `docs/feature-status.en.md`
- Test: all existing `test/` suites

- [ ] **Step 1: Document deployment migration and explicit rollback**

Document the following exact commands and behavior:

```bash
uv run python scripts/migrate_to_sqlite.py --data-dir ./data
uv run python scripts/migrate_to_sqlite.py --data-dir ./data --verify
```

State that `app.db` is the active source after migration, old JSON/legacy database files remain untouched, `STORAGE_BACKEND=json` is the explicit rollback switch, and admin key configuration is unchanged.

- [ ] **Step 2: Run the complete test suite**

Run: `uv run pytest -q`

Expected: exit code 0 and no test failures.

- [ ] **Step 3: Verify the active branch and clean unrelated work**

Run: `git status --short --branch`

Expected: branch is `feat/memory-optimization-sqlite`; only files from this plan are changed in this worktree. Changes in the outer main worktree are not staged or modified.

- [ ] **Step 4: Commit documentation and final migration verification**

```bash
git add docs/deployment.md docs/feature-status.en.md
git commit -m "docs: document unified sqlite state migration"
```
