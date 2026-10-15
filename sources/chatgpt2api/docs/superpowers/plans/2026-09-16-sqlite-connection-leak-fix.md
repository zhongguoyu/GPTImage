# SQLite Connection Leak Fix Implementation Plan

> For agentic workers: REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Stop SQLite connections created by short-lived refresh workers from accumulating for the lifetime of the process, while preserving current SQLite transaction and service concurrency behavior.

**Architecture:** Add a small SQLiteConnectionRegistry that owns one connection per live thread, tracks each owner with a weak thread reference, explicitly closes the calling thread's connection, and reaps connections whose threads exited without cleanup. Migrate the three SQLite stores to this lifecycle component and wrap account refresh workers in a finally cleanup; long-lived executors remain reusable and bounded by their configured worker count.

**Tech Stack:** Python 3.11+, stdlib sqlite3, threading, weakref, concurrent.futures, pytest/unittest, SQLite WAL.

---

## File Map

- Create: services/storage/sqlite_connection.py - shared per-store connection lifecycle registry.
- Modify: services/storage/sqlite_state.py - delegate connection creation and closing to the registry.
- Modify: services/scheduler/state_store.py - delegate scheduler state connections and keep writer cleanup semantics.
- Modify: services/quota_service.py - delegate quota connections and close the watchdog thread's connection on exit.
- Modify: services/account_service.py - close the SQLite storage connection in every account-refresh worker exit path.
- Create: test/test_sqlite_connection_registry.py - registry behavior and repeated short-lived executor regression tests.
- Modify: test/test_account_service.py - account refresh regression test against a real SQLite store.
- Modify: test/test_scheduler_state_store.py - stop relying on the private _connections implementation detail.
- Modify: test/test_quota_service.py - verify worker-created quota connections are reclaimed.
- Create: docs/superpowers/plans/2026-09-16-sqlite-connection-leak-fix.md - this implementation plan.

The existing uncommitted config.json is unrelated and must remain unstaged throughout execution.

## Task 1: Add failing lifecycle and regression tests

Files:
- Create: test/test_sqlite_connection_registry.py
- Modify: test/test_account_service.py
- Modify: test/test_scheduler_state_store.py
- Modify: test/test_quota_service.py

- [ ] Step 1: Write the registry tests first.

Create test/test_sqlite_connection_registry.py with tests that express the required behavior before the production module exists:

~~~python
from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Thread

import pytest

from services.storage.sqlite_connection import SQLiteConnectionRegistry


def make_registry(tmp_path: Path) -> SQLiteConnectionRegistry:
    db_path = tmp_path / "registry.db"

    def connect() -> sqlite3.Connection:
        connection = sqlite3.connect(str(db_path), check_same_thread=False)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    return SQLiteConnectionRegistry(connect, name="test")


def test_connection_is_reused_and_current_thread_close_is_idempotent(tmp_path):
    registry = make_registry(tmp_path)
    try:
        first = registry.connection()
        assert registry.connection() is first
        assert registry.connection_count() == 1

        registry.close_current_thread()
        registry.close_current_thread()

        assert registry.connection_count() == 0
        with pytest.raises(sqlite3.ProgrammingError):
            first.execute("SELECT 1")
    finally:
        registry.close()


def test_dead_thread_connection_is_reaped_before_new_connection(tmp_path):
    registry = make_registry(tmp_path)
    worker_connections = []
    try:
        def worker() -> None:
            connection = registry.connection()
            connection.execute("SELECT 1")
            worker_connections.append(connection)

        thread = Thread(target=worker)
        thread.start()
        thread.join()

        current = registry.connection()

        assert registry.connection_count() == 1
        assert current is not worker_connections[0]
        with pytest.raises(sqlite3.ProgrammingError):
            worker_connections[0].execute("SELECT 1")
    finally:
        registry.close()


def test_repeated_short_lived_executors_do_not_accumulate_connections(tmp_path):
    registry = make_registry(tmp_path)
    try:
        for _ in range(300):
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(registry.connection).result()
            assert registry.connection_count() == 1
            registry.reap_dead_threads()
            assert registry.connection_count() == 0
    finally:
        registry.close()
~~~

- [ ] Step 2: Add the account-refresh regression test.

Append this test to test/test_account_service.py. It uses a real SQLite store and a fake remote call, so the worker reaches the same update_account persistence path as production without making a network request:

~~~python

def test_refresh_accounts_releases_sqlite_worker_connections(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    service = AccountService(store)
    service.add_account_items([
        {"access_token": "refresh-token", "status": "正常", "quota": 3},
    ])
    baseline = store.connection_count()

    def fake_fetch(
        access_token: str,
        event: str,
        defer_invalid_removal: bool,
        *,
        quiet: bool = False,
    ):
        return service.update_account(
            access_token,
            {"status": "正常", "quota": 3},
            quiet=quiet,
        )

    service.fetch_remote_info = fake_fetch
    try:
        for _ in range(30):
            service.refresh_accounts(["refresh-token"], quiet=True)
            assert store.connection_count() == baseline
    finally:
        store.close()
~~~

- [ ] Step 3: Make the existing scheduler close test implementation-independent.

In test/test_scheduler_state_store.py, replace direct reads of store._connections with a list populated by the worker and the public diagnostic count:

~~~python
            connections = []

            def read() -> None:
                connections.append(store._connection())
                store.list_health()

            threads = [threading.Thread(target=read) for _ in range(4)]
~~~

After store.close(), assert store.connection_count() == 0; keep the existing ProgrammingError assertion for every saved connection.

- [ ] Step 4: Add a quota worker reclamation test.

Add a test to test/test_quota_service.py that creates an AuthQuotaStore, calls _connection() from a short-lived Thread, joins it, asserts the diagnostic count includes the worker connection, calls store.reap_dead_threads(), and then asserts the count is back to the main-thread baseline before closing the store. The store implementation must use check_same_thread=False so the reaper can close the worker connection.

- [ ] Step 5: Run only the new and changed tests to prove the red state.

Run:

~~~bash
PYTHONPATH=. /usr/bin/python3 -m pytest \
  test/test_sqlite_connection_registry.py \
  test/test_account_service.py \
  test/test_scheduler_state_store.py \
  test/test_quota_service.py -q
~~~

Expected: collection fails with ModuleNotFoundError: No module named services.storage.sqlite_connection because the registry production module has not been created yet. If the collection error is different, fix the test typo and rerun until the failure is caused by the missing behavior.

- [ ] Step 6: Commit the red tests.

~~~bash
git add test/test_sqlite_connection_registry.py test/test_account_service.py test/test_scheduler_state_store.py test/test_quota_service.py
git commit -m "test: reproduce sqlite worker connection leak"
~~~

## Task 2: Implement the connection registry

Files:
- Create: services/storage/sqlite_connection.py
- Test: test/test_sqlite_connection_registry.py

- [ ] Step 1: Implement the minimal registry API.

Create the module with this behavior:

~~~python
from __future__ import annotations

import logging
import sqlite3
import threading
import weakref
from collections.abc import Callable

logger = logging.getLogger(__name__)


class SQLiteConnectionRegistry:
    def __init__(
        self,
        connect: Callable[[], sqlite3.Connection],
        *,
        name: str,
    ) -> None:
        self._connect = connect
        self._name = str(name)
        self._local = threading.local()
        self._lock = threading.RLock()
        self._connections: dict[
            sqlite3.Connection,
            weakref.ReferenceType[threading.Thread],
        ] = {}

    def connection(self) -> sqlite3.Connection:
        existing = getattr(self._local, "connection", None)
        if existing is not None:
            return existing
        with self._lock:
            existing = getattr(self._local, "connection", None)
            if existing is not None:
                return existing
            self._reap_dead_threads_locked()
            connection = self._connect()
            self._connections[connection] = weakref.ref(threading.current_thread())
            self._local.connection = connection
            return connection

    def close_current_thread(self) -> None:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            return
        with self._lock:
            self._local.connection = None
            self._connections.pop(connection, None)
            self._close(connection)

    def reap_dead_threads(self) -> int:
        with self._lock:
            return self._reap_dead_threads_locked()

    def connection_count(self) -> int:
        with self._lock:
            return len(self._connections)

    def close(self) -> None:
        with self._lock:
            connections = tuple(self._connections)
            self._connections.clear()
            self._local.connection = None
            for connection in connections:
                self._close(connection)

    def _reap_dead_threads_locked(self) -> int:
        current = threading.current_thread()
        removed = 0
        for connection, owner_ref in tuple(self._connections.items()):
            owner = owner_ref()
            if owner is not None and owner is not current and owner.is_alive():
                continue
            self._connections.pop(connection, None)
            self._close(connection)
            removed += 1
        return removed

    def _close(self, connection: sqlite3.Connection) -> None:
        try:
            connection.close()
        except Exception:
            logger.warning(
                "sqlite_connection_close_failed",
                extra={"store": self._name},
                exc_info=True,
            )
~~~

The owner weak reference must be stored instead of a raw thread ident so an ident reuse cannot make a new thread appear to own an old connection. Cleanup must clear the current thread-local before closing and must be idempotent.

- [ ] Step 2: Run the registry tests.

Run:

~~~bash
PYTHONPATH=. /usr/bin/python3 -m pytest test/test_sqlite_connection_registry.py -q
~~~

Expected: all registry tests pass. If the dead-thread test fails because connection_count() reaps before the new connection is created, adjust only the test ordering so it verifies the old connection is closed and the live count is one.

- [ ] Step 3: Commit the registry implementation.

~~~bash
git add services/storage/sqlite_connection.py test/test_sqlite_connection_registry.py
git commit -m "fix: add bounded sqlite connection lifecycle"
~~~

## Task 3: Migrate all SQLite stores

Files:
- Modify: services/storage/sqlite_state.py
- Modify: services/scheduler/state_store.py
- Modify: services/quota_service.py
- Test: test/test_scheduler_state_store.py, test/test_quota_service.py, test/test_account_service.py

- [ ] Step 1: Migrate SQLiteStateStore.

Replace its _local and _connections initialization with a registry constructed from a store-local connection factory:

~~~python
self._connection_registry = SQLiteConnectionRegistry(
    self._create_connection,
    name="application-state",
)
~~~

Move the existing sqlite3.connect() and PRAGMA statements into _create_connection(). Keep _connection() as:

~~~python
def _connection(self) -> sqlite3.Connection:
    return self._connection_registry.connection()
~~~

Add:

~~~python
def close_current_thread(self) -> None:
    self._connection_registry.close_current_thread()

def connection_count(self) -> int:
    return self._connection_registry.connection_count()

def reap_dead_threads(self) -> int:
    return self._connection_registry.reap_dead_threads()
~~~

Replace close() body with self._connection_registry.close() while preserving the public method.

- [ ] Step 2: Migrate SchedulerStateStore.

Apply the same shape using name="scheduler-state". Preserve all scheduler PRAGMAs and the existing close_current_thread() public method. Add connection_count() and reap_dead_threads(). Do not alter SQL methods or transaction behavior.

- [ ] Step 3: Migrate AuthQuotaStore.

Apply the same shape using name="quota". Preserve isolation_level=None, explicit transaction commands, quota cache size, wal_autocheckpoint, and foreign key setup. Add close_current_thread(), connection_count(), and reap_dead_threads(), and delegate close() to the registry.

- [ ] Step 4: Run store tests.

Run:

~~~bash
PYTHONPATH=. /usr/bin/python3 -m pytest \
  test/test_sqlite_connection_registry.py \
  test/test_scheduler_state_store.py \
  test/test_quota_service.py -q
~~~

Expected: PASS for the registry, scheduler, and quota store tests. The account refresh regression remains the expected red test until Task 4 adds the worker wrapper. Existing SQLite WAL and transaction tests must remain green.

- [ ] Step 5: Commit the store migration.

~~~bash
git add services/storage/sqlite_state.py services/scheduler/state_store.py services/quota_service.py test/test_scheduler_state_store.py test/test_quota_service.py test/test_account_service.py
git commit -m "fix: use shared lifecycle for sqlite stores"
~~~

## Task 4: Close account refresh and quota watchdog worker connections

Files:
- Modify: services/account_service.py
- Modify: services/quota_service.py
- Test: test/test_account_service.py, test/test_quota_service.py

- [ ] Step 1: Add an account refresh worker wrapper.

Add a private method near fetch_remote_info():

~~~python
def _refresh_account_worker(
    self,
    access_token: str,
    defer_invalid_removal: bool,
    quiet: bool,
) -> dict[str, Any] | None:
    try:
        return self.fetch_remote_info(
            access_token,
            "refresh_accounts",
            defer_invalid_removal,
            quiet=quiet,
        )
    finally:
        close_current_thread = getattr(self.storage, "close_current_thread", None)
        if callable(close_current_thread):
            close_current_thread()
~~~

Change only the executor submission in refresh_accounts() to call this wrapper. Keep the futures map token association and all result/error handling unchanged. The wrapper must not catch or replace the worker exception.

- [ ] Step 2: Add quota watchdog exit cleanup.

Wrap the body of QuotaService.start_watchdog()'s run() function in try/finally and call self.store.close_current_thread() in the finally block. The existing periodic recovery behavior and error logging remain unchanged:

~~~python
def run() -> None:
    try:
        while not stop_event.wait(interval_seconds):
            try:
                self.recover_expired()
            except Exception as exc:
                logger.error({"event": "quota_reservation_recovery_failed", "error": str(exc)})
    finally:
        self.store.close_current_thread()
~~~

- [ ] Step 3: Run the targeted regression tests and confirm green.

Run:

~~~bash
PYTHONPATH=. /usr/bin/python3 -m pytest \
  test/test_sqlite_connection_registry.py \
  test/test_account_service.py \
  test/test_scheduler_state_store.py \
  test/test_quota_service.py -q
~~~

Expected: all selected tests pass, and the 30-cycle account refresh test keeps connection_count() at its baseline.

- [ ] Step 4: Commit worker cleanup.

~~~bash
git add services/account_service.py services/quota_service.py test/test_account_service.py test/test_quota_service.py
git commit -m "fix: release sqlite connections after refresh workers"
~~~

## Task 5: Verify complete behavior and prepare handoff

Files:
- Verify: all modified Python modules and tests.
- Do not modify: config.json.

- [ ] Step 1: Run syntax and focused tests.

Run:

~~~bash
PYTHONPATH=. /usr/bin/python3 -m compileall -q services/storage/sqlite_connection.py services/storage/sqlite_state.py services/scheduler/state_store.py services/quota_service.py services/account_service.py
PYTHONPATH=. /usr/bin/python3 -m pytest test/test_sqlite_connection_registry.py test/test_account_service.py test/test_scheduler_state_store.py test/test_quota_service.py -q
~~~

Expected: compileall exits 0 and the focused suite reports zero failures.

- [ ] Step 2: Run the full available test suite.

Run:

~~~bash
PYTHONPATH=. /usr/bin/python3 -m pytest -q
~~~

Expected: zero failures. If the environment cannot import an optional dependency, record the exact collection error and run every unaffected targeted suite; do not claim the full suite passed.

- [ ] Step 3: Inspect the diff and worktree.

Run:

~~~bash
git diff HEAD~3..HEAD --check
git diff HEAD~3..HEAD --stat
git status --short --branch
~~~

Expected: no whitespace errors; only the intended implementation, tests, plan, and prior spec commits are represented; config.json remains an unstaged user change.

- [ ] Step 4: Run a standalone 300-cycle resource regression.

Use the real SQLiteStateStore and one-worker executors to perform a SQLite operation, close the worker connection through the production wrapper, and assert connection_count() returns to the main-thread baseline after every cycle. This is the local equivalent of the observed 216 * 10 production growth pattern.

- [ ] Step 5: Request a code review checkpoint.

Review the final implementation against:
- the approved spec at docs/superpowers/specs/2026-09-16-sqlite-connection-leak-fix-design.md;
- this plan;
- the original production evidence;
- the focused and full test outputs.

Resolve all critical and important findings before reporting the implementation as complete.
