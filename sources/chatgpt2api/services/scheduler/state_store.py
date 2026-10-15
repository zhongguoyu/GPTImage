from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from services.storage.sqlite_connection import SQLiteConnectionRegistry


class SchedulerStateStore:
    """SQLite persistence for scheduler health snapshots and task metadata."""

    _MIGRATION_BATCH_SIZE = 500
    # Scheduler state is small metadata; avoid reserving several MiB per
    # request-thread connection while retaining SQLite's indexed hot paths.
    CACHE_SIZE_KIB = -1024
    _TASK_TABLE = "tasks"
    _SAFE_METADATA_KEYS = {
        "size",
        "quality",
        "n",
        "priority",
        "created_at",
        "conversation_id",
        "remote_task_id",
        "account_ref",
        "idempotency_key",
        "source",
        "quota_reservation_id",
        "quota_requested_units",
        "quota_status",
        "quota_committed_units",
    }
    _BLOCKED_METADATA_KEYS = {
        "body",
        "data",
        "image",
        "images",
        "mask",
        "prompt",
        "request",
        "request_body",
        "base64",
        "b64_json",
    }

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection_registry = SQLiteConnectionRegistry(
            self._create_connection,
            name="scheduler-state",
        )
        self._initialize_lock = threading.RLock()
        self._initialized = False

    def _create_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=5.0, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute(f"PRAGMA cache_size={self.CACHE_SIZE_KIB}")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _connection(self) -> sqlite3.Connection:
        return self._connection_registry.connection()

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            connection = self._connection()
            legacy_task_table_exists = self._table_exists("scheduler_tasks")
            with connection:
                connection.executescript(
                    """
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

                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version INTEGER PRIMARY KEY,
                        source_fingerprint TEXT NOT NULL DEFAULT '',
                        verification_json TEXT NOT NULL DEFAULT '{}',
                        completed_at REAL NOT NULL
                    );

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

                    CREATE INDEX IF NOT EXISTS idx_tasks_status_updated
                        ON tasks(status, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_tasks_owner_updated
                        ON tasks(owner_id, updated_at DESC);
                    """
                )
                if legacy_task_table_exists:
                    connection.execute(
                        """INSERT OR IGNORE INTO tasks
                           (task_key, owner_id, task_id, task_type, status, model,
                            remote_task_id, metadata_json, result_json, error,
                            created_at, updated_at)
                           SELECT task_key, owner_id, task_id, mode, status, model,
                                  remote_task_id, metadata_json, result_json, error,
                                  updated_at, updated_at
                           FROM scheduler_tasks"""
                    )
            self._initialized = True

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            self.initialize()

    def _table_exists(self, table: str) -> bool:
        return self._connection().execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (str(table),),
        ).fetchone() is not None

    def upsert_health(self, snapshot: dict[str, object]) -> None:
        self.upsert_health_batch([snapshot])

    def upsert_health_batch(self, snapshots: Iterable[dict[str, object]]) -> None:
        rows = [self._health_row(snapshot) for snapshot in snapshots]
        if not rows:
            return
        self._ensure_initialized()
        connection = self._connection()
        with connection:
            connection.executemany(
                """
                INSERT INTO scheduler_account_health (
                    account_key, lane, breaker_state, cooldown_until,
                    quota_snapshot, ewma_latency_ms, recent_success_count,
                    recent_failure_count, last_error_class, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_key, lane) DO UPDATE SET
                    breaker_state = excluded.breaker_state,
                    cooldown_until = excluded.cooldown_until,
                    quota_snapshot = excluded.quota_snapshot,
                    ewma_latency_ms = excluded.ewma_latency_ms,
                    recent_success_count = excluded.recent_success_count,
                    recent_failure_count = excluded.recent_failure_count,
                    last_error_class = excluded.last_error_class,
                    updated_at = excluded.updated_at
                """,
                rows,
            )

    def list_health(self) -> list[dict[str, object]]:
        self._ensure_initialized()
        rows = self._connection().execute(
            """
            SELECT account_key, lane, breaker_state, cooldown_until,
                   quota_snapshot, ewma_latency_ms, recent_success_count,
                   recent_failure_count, last_error_class, updated_at
            FROM scheduler_account_health
            ORDER BY account_key, lane
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def save_task(self, task: dict[str, object]) -> dict[str, object]:
        record = self._task_record(task)
        self._ensure_initialized()
        connection = self._connection()
        with connection:
            connection.execute(
                """
                INSERT INTO tasks (
                    task_key, owner_id, task_id, task_type, status, model,
                    remote_task_id, metadata_json, result_json, error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_key) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    task_id = excluded.task_id,
                    task_type = excluded.task_type,
                    status = excluded.status,
                    model = excluded.model,
                    remote_task_id = excluded.remote_task_id,
                    metadata_json = excluded.metadata_json,
                    result_json = excluded.result_json,
                    error = excluded.error,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at
                """,
                record,
            )
        return self.get_task(str(record[0]))

    def save_task_batch(self, tasks: Iterable[dict[str, object]]) -> None:
        records = [self._task_record(task) for task in tasks]
        if not records:
            return
        self._ensure_initialized()
        connection = self._connection()
        with connection:
            connection.executemany(
                """
                INSERT INTO tasks (
                    task_key, owner_id, task_id, task_type, status, model,
                    remote_task_id, metadata_json, result_json, error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_key) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    task_id = excluded.task_id,
                    task_type = excluded.task_type,
                    status = excluded.status,
                    model = excluded.model,
                    remote_task_id = excluded.remote_task_id,
                    metadata_json = excluded.metadata_json,
                    result_json = excluded.result_json,
                    error = excluded.error,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at
                """,
                records,
            )

    def update_task(self, task_key: str, updates: dict[str, object]) -> dict[str, object] | None:
        self._ensure_initialized()
        current = self.get_task(task_key)
        if current is None:
            return None
        merged = dict(current)
        merged.update(updates)
        merged["task_key"] = task_key
        if "metadata" in updates:
            merged["metadata"] = self._safe_metadata(updates.get("metadata"))
        if "result" in updates:
            merged["result"] = self._safe_json_value(updates.get("result"))
        return self.save_task(merged)

    def get_task(self, task_key: str) -> dict[str, object] | None:
        self._ensure_initialized()
        row = self._connection().execute(
            "SELECT * FROM tasks WHERE task_key = ?",
            (str(task_key),),
        ).fetchone()
        return self._decode_task(row) if row is not None else None

    def list_tasks(
        self,
        owner_id: str | None = None,
        *,
        limit: int = 1000,
        task_types: Iterable[str] | None = None,
    ) -> list[dict[str, object]]:
        self._ensure_initialized()
        limit = max(1, min(int(limit), 10000))
        normalized_types = tuple(
            sorted({str(task_type).strip() for task_type in (task_types or ()) if str(task_type).strip()})
        )
        type_clause = ""
        type_parameters: tuple[object, ...] = ()
        if normalized_types:
            type_clause = f" AND task_type IN ({','.join('?' for _ in normalized_types)})"
            type_parameters = normalized_types
        if owner_id:
            rows = self._connection().execute(
                f"SELECT * FROM tasks WHERE owner_id = ?{type_clause} ORDER BY updated_at DESC LIMIT ?",
                (str(owner_id), *type_parameters, limit),
            ).fetchall()
        else:
            rows = self._connection().execute(
                f"SELECT * FROM tasks WHERE 1 = 1{type_clause} ORDER BY updated_at DESC LIMIT ?",
                (*type_parameters, limit),
            ).fetchall()
        return [self._decode_task(row) for row in rows]

    def delete_task(self, task_key: str) -> None:
        self._ensure_initialized()
        connection = self._connection()
        with connection:
            connection.execute(
                "DELETE FROM tasks WHERE task_key = ?",
                (str(task_key),),
            )

    def delete_tasks(self, task_keys: Iterable[str]) -> None:
        keys = [(str(task_key),) for task_key in task_keys if str(task_key).strip()]
        if not keys:
            return
        self._ensure_initialized()
        connection = self._connection()
        with connection:
            connection.executemany(
                "DELETE FROM tasks WHERE task_key = ?",
                keys,
            )

    def load_recoverable_tasks(
        self,
        *,
        limit: int = 1000,
        task_types: Iterable[str] | None = None,
    ) -> list[dict[str, object]]:
        self._ensure_initialized()
        limit = max(1, min(int(limit), 10000))
        normalized_types = tuple(
            sorted({str(task_type).strip() for task_type in (task_types or ()) if str(task_type).strip()})
        )
        type_clause = ""
        type_parameters: tuple[object, ...] = ()
        if normalized_types:
            type_clause = f" AND task_type IN ({','.join('?' for _ in normalized_types)})"
            type_parameters = normalized_types
        rows = self._connection().execute(
            f"""
            SELECT * FROM tasks
            WHERE status IN ('queued', 'running', 'unknown')
            {type_clause}
            ORDER BY updated_at ASC
            LIMIT ?
            """,
            (*type_parameters, limit),
        ).fetchall()
        items = []
        unknown_keys: list[str] = []
        for row in rows:
            item = self._decode_task(row)
            if (
                item["status"] in {"queued", "running"}
                and not str(item.get("remote_task_id") or "").strip()
            ):
                item["status"] = "unknown"
                unknown_keys.append(str(item["task_key"]))
            item["retryable"] = bool(
                str(item.get("remote_task_id") or "").strip()
            )
            items.append(item)
        if unknown_keys:
            connection = self._connection()
            with connection:
                connection.executemany(
                    "UPDATE tasks SET status = 'unknown' WHERE task_key = ?",
                    [(task_key,) for task_key in unknown_keys],
                )
        return items

    def cleanup_tasks(
        self,
        *,
        now: float,
        retention_seconds: int,
        max_rows: int,
        task_types: Iterable[str] | None = None,
    ) -> int:
        self._ensure_initialized()
        terminal_statuses = ("success", "completed", "error", "failed", "cancelled")
        cutoff = float(now) - max(0, int(retention_seconds))
        normalized_types = tuple(
            sorted({str(task_type).strip() for task_type in (task_types or ()) if str(task_type).strip()})
        )
        type_clause = ""
        type_parameters: tuple[object, ...] = ()
        if normalized_types:
            type_clause = f" AND task_type IN ({','.join('?' for _ in normalized_types)})"
            type_parameters = normalized_types
        connection = self._connection()
        removed = 0
        with connection:
            cursor = connection.execute(
                f"""DELETE FROM tasks
                   WHERE status IN (?, ?, ?, ?, ?)
                     AND updated_at < ?{type_clause}""",
                (*terminal_statuses, cutoff, *type_parameters),
            )
            removed += int(cursor.rowcount or 0)
            max_rows = max(0, int(max_rows))
            if max_rows == 0:
                cursor = connection.execute(
                    f"DELETE FROM tasks WHERE status IN (?, ?, ?, ?, ?){type_clause}",
                    (*terminal_statuses, *type_parameters),
                )
                removed += int(cursor.rowcount or 0)
            else:
                old_rows = connection.execute(
                    f"""SELECT task_key FROM tasks
                       WHERE status IN (?, ?, ?, ?, ?)
                       {type_clause}
                       ORDER BY updated_at DESC
                       LIMIT -1 OFFSET ?""",
                    (*terminal_statuses, *type_parameters, max_rows),
                ).fetchall()
                if old_rows:
                    connection.executemany(
                        "DELETE FROM tasks WHERE task_key = ?",
                        [(str(row[0]),) for row in old_rows],
                    )
                    removed += len(old_rows)
        return removed

    def import_legacy_tasks(self, path: Path) -> dict[str, object]:
        source = Path(path)
        if not source.exists():
            return {"imported": 0, "skipped": 0, "path": None}
        raw = json.loads(source.read_text(encoding="utf-8"))
        raw_items = raw.get("tasks") if isinstance(raw, dict) else raw
        if not isinstance(raw_items, list):
            raise ValueError("legacy image task file must contain a task list")

        self._ensure_initialized()
        imported = 0
        skipped = 0
        batch: list[tuple[object, ...]] = []
        connection = self._connection()
        for item in raw_items:
            if not isinstance(item, dict):
                skipped += 1
                continue
            try:
                record = self._legacy_task_record(item)
            except ValueError:
                skipped += 1
                continue
            batch.append(record)
            if len(batch) >= self._MIGRATION_BATCH_SIZE:
                imported_batch, skipped_batch = self._insert_task_batch(connection, batch)
                imported += imported_batch
                skipped += skipped_batch
                batch.clear()
        if batch:
            imported_batch, skipped_batch = self._insert_task_batch(connection, batch)
            imported += imported_batch
            skipped += skipped_batch

        migrated = self._migration_path(source)
        source.replace(migrated)
        return {"imported": imported, "skipped": skipped, "path": str(migrated)}

    def close(self) -> None:
        with self._initialize_lock:
            self._connection_registry.close()

    def close_current_thread(self) -> None:
        self._connection_registry.close_current_thread()

    def reap_dead_threads(self) -> int:
        return self._connection_registry.reap_dead_threads()

    def connection_count(self) -> int:
        return self._connection_registry.connection_count()

    def has_migration_marker(self, version: int = 1) -> bool:
        self._ensure_initialized()
        return self._connection().execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (int(version),),
        ).fetchone() is not None

    @staticmethod
    def _health_row(snapshot: dict[str, object]) -> tuple[object, ...]:
        account_key = str(snapshot.get("account_key") or "").strip()
        lane = str(snapshot.get("lane") or "").strip()
        if not account_key or not lane:
            raise ValueError("health snapshot requires account_key and lane")
        return (
            account_key,
            lane,
            str(snapshot.get("breaker_state") or "closed"),
            snapshot.get("cooldown_until"),
            snapshot.get("quota_snapshot"),
            float(snapshot.get("ewma_latency_ms") or 0),
            int(snapshot.get("recent_success_count") or 0),
            int(snapshot.get("recent_failure_count") or 0),
            str(snapshot.get("last_error_class") or ""),
            float(snapshot.get("updated_at") or time.time()),
        )

    def _task_record(self, task: dict[str, object]) -> tuple[object, ...]:
        task_key = str(task.get("task_key") or "").strip()
        owner_id = str(task.get("owner_id") or "").strip()
        task_id = str(task.get("task_id") or task.get("id") or "").strip()
        if task_key and (not owner_id or not task_id):
            owner_id, separator, task_id = task_key.partition(":")
            if not separator or not owner_id or not task_id:
                raise ValueError("task_key must use owner_id:task_id format")
        if not task_key:
            if not owner_id or not task_id:
                raise ValueError("task requires task_key or owner_id and task_id")
            task_key = f"{owner_id}:{task_id}"
        if not owner_id or not task_id:
            raise ValueError("task requires owner_id and task_id")
        return (
            task_key,
            owner_id,
            task_id,
            str(task.get("mode") or task.get("task_type") or "generate"),
            str(task.get("status") or "queued"),
            str(task.get("model") or ""),
            str(task.get("remote_task_id") or "").strip() or None,
            json.dumps(
                self._safe_metadata(task.get("metadata")),
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            self._json_text(self._safe_json_value(task.get("result"))),
            str(task.get("error") or ""),
            self._timestamp(task.get("created_at_ts") or task.get("created_at") or task.get("updated_at")),
            self._timestamp(task.get("updated_at")),
        )

    @staticmethod
    def _timestamp(value: object) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return time.time()

    def _decode_task(self, row: sqlite3.Row) -> dict[str, object]:
        metadata = self._decode_json(row["metadata_json"], {})
        result = self._decode_json(row["result_json"], None)
        return {
            "task_key": row["task_key"],
            "owner_id": row["owner_id"],
            "task_id": row["task_id"],
            "mode": row["task_type"],
            "status": row["status"],
            "model": row["model"],
            "remote_task_id": row["remote_task_id"],
            "metadata": metadata,
            "result": result,
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _legacy_task_record(self, item: dict[str, object]) -> tuple[object, ...]:
        owner_id = str(item.get("owner_id") or "").strip()
        task_id = str(item.get("id") or item.get("task_id") or "").strip()
        if not owner_id or not task_id:
            raise ValueError("legacy task requires owner_id and id")
        remote_task_id = str(
            item.get("remote_task_id") or item.get("conversation_id") or ""
        ).strip() or None
        status = str(item.get("status") or "queued").strip()
        if status in {"queued", "running"} and not remote_task_id:
            status = "unknown"
        metadata = {
            key: item[key]
            for key in self._SAFE_METADATA_KEYS
            if key in item and key not in self._BLOCKED_METADATA_KEYS
        }
        if remote_task_id:
            metadata["remote_task_id"] = remote_task_id
        return (
            f"{owner_id}:{task_id}",
            owner_id,
            task_id,
            "edit" if item.get("mode") == "edit" else "generate",
            status,
            str(item.get("model") or "gpt-image-2"),
            remote_task_id,
            json.dumps(self._safe_metadata(metadata), ensure_ascii=False, separators=(",", ":")),
            None,
            str(item.get("error") or ""),
            self._timestamp(item.get("created_ts") or item.get("updated_ts")),
            self._timestamp(item.get("updated_ts")),
        )

    @staticmethod
    def _insert_task_batch(
        connection: sqlite3.Connection,
        rows: list[tuple[object, ...]],
    ) -> tuple[int, int]:
        if not rows:
            return 0, 0
        with connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO tasks (
                    task_key, owner_id, task_id, task_type, status, model,
                    remote_task_id, metadata_json, result_json, error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            imported = connection.total_changes - before
        return imported, len(rows) - imported

    @staticmethod
    def _safe_metadata(value: object) -> dict[str, object]:
        if not isinstance(value, dict):
            return {}
        result: dict[str, object] = {}
        for key, item in value.items():
            normalized_key = str(key).strip()
            if not normalized_key or normalized_key.lower() in SchedulerStateStore._BLOCKED_METADATA_KEYS:
                continue
            if normalized_key not in SchedulerStateStore._SAFE_METADATA_KEYS:
                continue
            safe = SchedulerStateStore._safe_json_value(item)
            if safe is not None:
                result[normalized_key] = safe
        return result

    @staticmethod
    def _safe_json_value(value: object) -> object:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            return [
                safe
                for item in value
                if (safe := SchedulerStateStore._safe_json_value(item)) is not None
            ]
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                normalized_key = str(key).lower()
                if normalized_key in SchedulerStateStore._BLOCKED_METADATA_KEYS:
                    continue
                safe = SchedulerStateStore._safe_json_value(item)
                if safe is not None:
                    result[str(key)] = safe
            return result
        return None

    @staticmethod
    def _json_text(value: object) -> str | None:
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _decode_json(value: object, default: object) -> object:
        if not value:
            return default
        try:
            return json.loads(str(value))
        except (TypeError, json.JSONDecodeError):
            return default

    @staticmethod
    def _migration_path(source: Path) -> Path:
        candidate = source.with_name(source.name + ".migrated")
        if not candidate.exists():
            return candidate
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        candidate = source.with_name(f"{source.name}.migrated.{timestamp}")
        suffix = 1
        while candidate.exists():
            candidate = source.with_name(f"{source.name}.migrated.{timestamp}.{suffix}")
            suffix += 1
        return candidate


class SnapshotWriter:
    """Coalesce health changes and flush them from one background thread."""

    def __init__(
        self,
        store: SchedulerStateStore,
        *,
        interval_seconds: float = 1.0,
        pending_threshold: int = 100,
    ):
        self.store = store
        self.interval_seconds = max(0.001, float(interval_seconds))
        self.pending_threshold = max(1, int(pending_threshold))
        self._lock = threading.RLock()
        self._pending: dict[tuple[str, str], dict[str, object]] = {}
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def submit(self, snapshot: dict[str, object]) -> None:
        account_key = str(snapshot.get("account_key") or "").strip()
        lane = str(snapshot.get("lane") or "").strip()
        if not account_key or not lane:
            raise ValueError("health snapshot requires account_key and lane")
        with self._lock:
            self._pending[(account_key, lane)] = dict(snapshot)
            if len(self._pending) >= self.pending_threshold:
                self._wake.set()

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="scheduler-health-snapshot-writer",
                daemon=True,
            )
            self._thread.start()

    def flush(self) -> None:
        with self._lock:
            batch = list(self._pending.values())
            self._pending.clear()
        if not batch:
            return
        try:
            self.store.upsert_health_batch(batch)
        except Exception:
            with self._lock:
                for item in batch:
                    key = (str(item["account_key"]), str(item["lane"]))
                    self._pending.setdefault(key, item)
            raise

    def stop(self, timeout: float | None = None) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        if thread is None or not thread.is_alive():
            self._thread = None
        self.flush()

    def _run(self) -> None:
        try:
            while True:
                self._wake.wait(self.interval_seconds)
                self._wake.clear()
                if self._stop.is_set():
                    break
                try:
                    self.flush()
                except Exception:
                    continue
            try:
                self.flush()
            except Exception:
                pass
        finally:
            close_current_thread = getattr(self.store, "close_current_thread", None)
            if callable(close_current_thread):
                close_current_thread()
            else:
                self.store.close()


class TaskStateWriter:
    """Coalesce task metadata updates outside scheduler and image workers."""

    def __init__(
        self,
        store: SchedulerStateStore,
        *,
        interval_seconds: float = 1.0,
        pending_threshold: int = 100,
    ):
        self.store = store
        self.interval_seconds = max(0.001, float(interval_seconds))
        self.pending_threshold = max(1, int(pending_threshold))
        self._lock = threading.RLock()
        self._db_lock = threading.RLock()
        self._pending: dict[str, dict[str, object]] = {}
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def submit(self, task: dict[str, object]) -> None:
        task_key = str(task.get("task_key") or "").strip()
        if not task_key:
            raise ValueError("task state requires task_key")
        with self._lock:
            self._pending[task_key] = dict(task)
            if len(self._pending) >= self.pending_threshold:
                self._wake.set()

    def discard(self, task_key: str) -> None:
        with self._lock:
            self._pending.pop(str(task_key), None)

    def delete(self, task_key: str) -> None:
        """Delete a task after serializing with any in-flight writer flush."""
        normalized = str(task_key)
        with self._db_lock:
            with self._lock:
                self._pending.pop(normalized, None)
            self.store.delete_task(normalized)

    def delete_tasks(self, task_keys: Iterable[str]) -> None:
        normalized = [str(task_key) for task_key in task_keys if str(task_key).strip()]
        if not normalized:
            return
        with self._db_lock:
            with self._lock:
                for task_key in normalized:
                    self._pending.pop(task_key, None)
            self.store.delete_tasks(normalized)

    def save_now(self, task: dict[str, object]) -> None:
        """Synchronously persist one task in the same order as queued writes."""
        task_key = str(task.get("task_key") or "").strip()
        if not task_key:
            raise ValueError("task state requires task_key")
        with self._db_lock:
            with self._lock:
                self._pending.pop(task_key, None)
            self.store.save_task(task)

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def start(self) -> None:
        with self._lock:
            if self.running:
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="scheduler-task-state-writer",
                daemon=True,
            )
            self._thread.start()

    def flush(self) -> None:
        with self._db_lock:
            with self._lock:
                batch = list(self._pending.values())
                self._pending.clear()
            if not batch:
                return
            try:
                self.store.save_task_batch(batch)
            except Exception:
                with self._lock:
                    for item in batch:
                        task_key = str(item["task_key"])
                        self._pending.setdefault(task_key, item)
                raise

    def stop(self, timeout: float | None = None) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        if thread is None or not thread.is_alive():
            self._thread = None
        self.flush()

    def _run(self) -> None:
        try:
            while True:
                self._wake.wait(self.interval_seconds)
                self._wake.clear()
                if self._stop.is_set():
                    break
                try:
                    self.flush()
                except Exception:
                    continue
        finally:
            close_current_thread = getattr(self.store, "close_current_thread", None)
            if callable(close_current_thread):
                close_current_thread()
            else:
                self.store.close()
