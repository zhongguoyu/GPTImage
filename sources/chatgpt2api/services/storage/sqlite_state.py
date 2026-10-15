from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from services.storage.base import StorageBackend
from services.storage.sqlite_connection import SQLiteConnectionRegistry


class SQLiteStateStore(StorageBackend):
    """Normalized SQLite storage for application state.

    The compatibility methods return the dictionaries expected by the existing
    services, while writes update rows and child tables instead of serializing a
    complete accounts snapshot to JSON.
    """

    # Application state is metadata-heavy rather than scan-heavy. A smaller
    # per-thread page cache prevents request workers from reserving 4 MiB each.
    CACHE_SIZE_KIB = -1024

    _ACCOUNT_COLUMNS = {
        "access_token",
        "refresh_token",
        "id_token",
        "email",
        "user_id",
        "type",
        "source_type",
        "export_type",
        "proxy",
        "password",
        "status",
        "quota",
        "default_model_slug",
        "restore_at",
        "success",
        "fail",
        "invalid_count",
        "last_used_at",
        "last_invalid_at",
        "last_refresh_error",
        "last_refresh_error_at",
        "last_token_refresh_at",
        "last_token_refresh_error",
        "last_token_refresh_error_at",
        "created_at",
        "token_generation",
    }

    def __init__(
        self,
        db_path: Path,
        *,
        legacy_data_dir: Path | None = None,
        auto_migrate: bool = True,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.legacy_data_dir = Path(legacy_data_dir) if legacy_data_dir else None
        self.auto_migrate = auto_migrate
        self._connection_registry = SQLiteConnectionRegistry(
            self._create_connection,
            name="application-state",
        )
        self._initialize_lock = threading.RLock()
        self._write_lock = threading.RLock()
        self._initialized = False

    def _create_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=5.0,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute(f"PRAGMA cache_size={self.CACHE_SIZE_KIB}")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _connection(self) -> sqlite3.Connection:
        return self._connection_registry.connection()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self._ensure_initialized()
        connection = self._connection()
        with self._write_lock:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            connection = self._connection()
            connection.executescript(
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
                    email TEXT,
                    user_id TEXT,
                    type TEXT NOT NULL DEFAULT '',
                    source_type TEXT NOT NULL DEFAULT '',
                    export_type TEXT NOT NULL DEFAULT '',
                    proxy TEXT NOT NULL DEFAULT '',
                    password TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    quota INTEGER NOT NULL DEFAULT 0,
                    default_model_slug TEXT,
                    restore_at TEXT,
                    success INTEGER NOT NULL DEFAULT 0,
                    fail INTEGER NOT NULL DEFAULT 0,
                    invalid_count INTEGER NOT NULL DEFAULT 0,
                    last_used_at TEXT,
                    last_invalid_at TEXT,
                    last_refresh_error TEXT,
                    last_refresh_error_at TEXT,
                    last_token_refresh_at TEXT,
                    last_token_refresh_error TEXT,
                    last_token_refresh_error_at TEXT,
                    created_at TEXT,
                    token_generation INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL,
                    extra_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_accounts_status
                    ON accounts(status);
                CREATE INDEX IF NOT EXISTS idx_accounts_email
                    ON accounts(email);

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
                CREATE INDEX IF NOT EXISTS idx_account_token_aliases_expiry
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
                    text_limit INTEGER NOT NULL DEFAULT 0 CHECK (text_limit >= 0),
                    text_used INTEGER NOT NULL DEFAULT 0 CHECK (text_used >= 0),
                    text_reserved INTEGER NOT NULL DEFAULT 0 CHECK (text_reserved >= 0),
                    image_limit INTEGER NOT NULL DEFAULT 0 CHECK (image_limit >= 0),
                    image_used INTEGER NOT NULL DEFAULT 0 CHECK (image_used >= 0),
                    image_reserved INTEGER NOT NULL DEFAULT 0 CHECK (image_reserved >= 0),
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    deleted_at REAL
                );

                CREATE TABLE IF NOT EXISTS quota_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    key_id TEXT NOT NULL,
                    quota_type TEXT NOT NULL CHECK (quota_type IN ('text', 'image')),
                    requested_units INTEGER NOT NULL CHECK (requested_units > 0),
                    committed_units INTEGER NOT NULL DEFAULT 0 CHECK (committed_units >= 0),
                    status TEXT NOT NULL CHECK (status IN ('reserved', 'committed', 'released', 'expired')),
                    request_id TEXT,
                    endpoint TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    settled_at REAL
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
            connection.commit()
            self._initialized = True

        if self.auto_migrate and self.legacy_data_dir is not None:
            from services.storage.sqlite_migration import migrate_legacy_state

            migrate_legacy_state(self.legacy_data_dir, self.db_path)

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            self.initialize()

    def table_names(self) -> set[str]:
        self._ensure_initialized()
        rows = self._connection().execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
        return {str(row[0]) for row in rows}

    def scalar(self, sql: str, parameters: tuple[object, ...] = ()) -> object:
        self._ensure_initialized()
        row = self._connection().execute(sql, parameters).fetchone()
        return None if row is None else row[0]

    @staticmethod
    def _account_id(access_token: str) -> str:
        return hashlib.sha256(access_token.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _json_object(value: object) -> dict[str, object]:
        if not isinstance(value, dict):
            return {}
        return value

    @staticmethod
    def _json_text(value: object) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _int_value(value: object, default: int = 0) -> int:
        try:
            return int(value if value is not None else default)
        except (TypeError, ValueError):
            return default

    @classmethod
    def _account_values(cls, account: dict[str, Any], account_id: str) -> tuple[object, ...]:
        extras = {
            str(key): value
            for key, value in account.items()
            if str(key) not in cls._ACCOUNT_COLUMNS and str(key) not in {"accessToken", "limits_progress"}
        }
        return (
            account_id,
            str(account.get("access_token") or account.get("accessToken") or "").strip(),
            str(account.get("refresh_token") or ""),
            str(account.get("id_token") or ""),
            account.get("email") or None,
            account.get("user_id") or None,
            str(account.get("type") or ""),
            str(account.get("source_type") or ""),
            str(account.get("export_type") or ""),
            str(account.get("proxy") or ""),
            str(account.get("password") or ""),
            str(account.get("status") or ""),
            cls._int_value(account.get("quota")),
            account.get("default_model_slug") or None,
            account.get("restore_at") or None,
            cls._int_value(account.get("success")),
            cls._int_value(account.get("fail")),
            cls._int_value(account.get("invalid_count")),
            account.get("last_used_at"),
            account.get("last_invalid_at") or None,
            account.get("last_refresh_error") or None,
            account.get("last_refresh_error_at") or None,
            account.get("last_token_refresh_at") or None,
            account.get("last_token_refresh_error") or None,
            account.get("last_token_refresh_error_at") or None,
            account.get("created_at") or None,
            cls._int_value(account.get("token_generation")),
            time.time(),
            cls._json_text(extras),
        )

    @classmethod
    def _decode_limits(cls, connection: sqlite3.Connection, account_id: str) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        rows = connection.execute(
            "SELECT capability, remaining, reset_at, raw_json FROM account_limits WHERE account_id = ? ORDER BY capability",
            (account_id,),
        ).fetchall()
        for row in rows:
            try:
                value = json.loads(row["raw_json"])
            except (TypeError, json.JSONDecodeError):
                value = {}
            if not isinstance(value, dict):
                value = {}
            value.setdefault("name", row["capability"])
            if row["remaining"] is not None:
                value.setdefault("remaining", row["remaining"])
            if row["reset_at"] is not None and "reset_at" not in value and "reset_after" not in value:
                value.setdefault("reset_at", row["reset_at"])
            result.append(value)
        return result

    @classmethod
    def _decode_account(cls, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        try:
            extras = json.loads(row["extra_json"])
        except (TypeError, json.JSONDecodeError):
            extras = {}
        account = dict(extras) if isinstance(extras, dict) else {}
        for column in cls._ACCOUNT_COLUMNS:
            value = row[column]
            if column in {"refresh_token", "id_token", "type", "source_type", "export_type", "proxy", "password", "status"}:
                account[column] = str(value or "")
            elif column in {"quota", "success", "fail", "invalid_count", "token_generation"}:
                account[column] = int(value or 0)
            elif value is not None:
                account[column] = value
        account["limits_progress"] = cls._decode_limits(connection, str(row["account_id"]))
        return account

    @classmethod
    def _limit_rows(cls, account: dict[str, Any], account_id: str) -> list[tuple[object, ...]]:
        rows: list[tuple[object, ...]] = []
        limits = account.get("limits_progress")
        if not isinstance(limits, list):
            return rows
        for index, item in enumerate(limits):
            if not isinstance(item, dict):
                continue
            capability = str(
                item.get("name")
                or item.get("capability")
                or item.get("feature")
                or item.get("type")
                or f"feature-{index}"
            ).strip()
            if not capability:
                continue
            remaining = item.get("remaining")
            if remaining is None:
                remaining = item.get("quota")
            reset_at = item.get("reset_at")
            if reset_at is None:
                reset_at = item.get("reset_after")
            rows.append(
                (
                    account_id,
                    capability,
                    cls._int_value(remaining, default=0) if remaining is not None else None,
                    float(reset_at) if isinstance(reset_at, (int, float)) else None,
                    cls._json_text(item),
                )
            )
        return rows

    def _find_account_id(self, connection: sqlite3.Connection, access_token: str) -> str:
        row = connection.execute(
            "SELECT account_id FROM accounts WHERE access_token = ?",
            (access_token,),
        ).fetchone()
        return str(row[0]) if row is not None else self._account_id(access_token)

    def _save_account_in_connection(self, connection: sqlite3.Connection, account: dict[str, Any]) -> str:
        access_token = str(account.get("access_token") or account.get("accessToken") or "").strip()
        if not access_token:
            raise ValueError("account access_token is required")
        account_id = self._find_account_id(connection, access_token)
        values = self._account_values({**account, "access_token": access_token}, account_id)
        connection.execute(
            """INSERT INTO accounts (
                account_id, access_token, refresh_token, id_token, email, user_id,
                type, source_type, export_type, proxy, password, status, quota,
                default_model_slug, restore_at, success, fail, invalid_count,
                last_used_at, last_invalid_at, last_refresh_error,
                last_refresh_error_at, last_token_refresh_at,
                last_token_refresh_error, last_token_refresh_error_at, created_at,
                token_generation, updated_at, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(access_token) DO UPDATE SET
                refresh_token=excluded.refresh_token,
                id_token=excluded.id_token,
                email=excluded.email,
                user_id=excluded.user_id,
                type=excluded.type,
                source_type=excluded.source_type,
                export_type=excluded.export_type,
                proxy=excluded.proxy,
                password=excluded.password,
                status=excluded.status,
                quota=excluded.quota,
                default_model_slug=excluded.default_model_slug,
                restore_at=excluded.restore_at,
                success=excluded.success,
                fail=excluded.fail,
                invalid_count=excluded.invalid_count,
                last_used_at=excluded.last_used_at,
                last_invalid_at=excluded.last_invalid_at,
                last_refresh_error=excluded.last_refresh_error,
                last_refresh_error_at=excluded.last_refresh_error_at,
                last_token_refresh_at=excluded.last_token_refresh_at,
                last_token_refresh_error=excluded.last_token_refresh_error,
                last_token_refresh_error_at=excluded.last_token_refresh_error_at,
                created_at=COALESCE(accounts.created_at, excluded.created_at),
                token_generation=excluded.token_generation,
                updated_at=excluded.updated_at,
                extra_json=excluded.extra_json""",
            values,
        )
        account_id = self._find_account_id(connection, access_token)
        connection.execute("DELETE FROM account_limits WHERE account_id = ?", (account_id,))
        connection.executemany(
            """INSERT INTO account_limits
               (account_id, capability, remaining, reset_at, raw_json)
               VALUES (?, ?, ?, ?, ?)""",
            self._limit_rows(account, account_id),
        )
        return account_id

    def load_accounts(self) -> list[dict[str, Any]]:
        self._ensure_initialized()
        rows = self._connection().execute(
            "SELECT * FROM accounts ORDER BY rowid"
        ).fetchall()
        return [self._decode_account(self._connection(), row) for row in rows]

    def save_account(self, account: dict[str, Any]) -> None:
        with self.transaction() as connection:
            self._save_account_in_connection(connection, account)

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        valid = [item for item in accounts if isinstance(item, dict)]
        tokens = {
            str(item.get("access_token") or item.get("accessToken") or "").strip()
            for item in valid
        }
        tokens.discard("")
        with self.transaction() as connection:
            for item in valid:
                if str(item.get("access_token") or item.get("accessToken") or "").strip():
                    self._save_account_in_connection(connection, item)
            if not tokens:
                connection.execute("DELETE FROM accounts")
            else:
                placeholders = ",".join("?" for _ in tokens)
                connection.execute(
                    f"DELETE FROM accounts WHERE access_token NOT IN ({placeholders})",
                    tuple(tokens),
                )

    def delete_account(self, access_token: str) -> None:
        with self.transaction() as connection:
            connection.execute("DELETE FROM accounts WHERE access_token = ?", (str(access_token),))

    @classmethod
    def _auth_key_values(cls, item: dict[str, Any]) -> tuple[object, ...]:
        known = {"id", "name", "role", "key_hash", "enabled", "web_login_enabled", "created_at", "last_used_at"}
        extras = {str(key): value for key, value in item.items() if str(key) not in known}
        return (
            str(item.get("id") or "").strip(),
            str(item.get("name") or ""),
            str(item.get("role") or "user"),
            str(item.get("key_hash") or ""),
            1 if bool(item.get("enabled", True)) else 0,
            1 if bool(item.get("web_login_enabled", True)) else 0,
            str(item.get("created_at") or ""),
            item.get("last_used_at"),
            cls._json_text(extras),
        )

    @classmethod
    def _decode_auth_key(cls, row: sqlite3.Row) -> dict[str, object]:
        try:
            extras = json.loads(row["extra_json"])
        except (TypeError, json.JSONDecodeError):
            extras = {}
        result = dict(extras) if isinstance(extras, dict) else {}
        result.update(
            {
                "id": row["key_id"],
                "name": row["name"],
                "role": row["role"],
                "key_hash": row["key_hash"],
                "enabled": bool(row["enabled"]),
                "web_login_enabled": bool(row["web_login_enabled"]),
                "created_at": row["created_at"],
                "last_used_at": row["last_used_at"],
            }
        )
        return result

    def load_auth_keys(self) -> list[dict[str, Any]]:
        self._ensure_initialized()
        rows = self._connection().execute(
            "SELECT * FROM auth_keys ORDER BY rowid"
        ).fetchall()
        return [self._decode_auth_key(row) for row in rows]

    def save_auth_key(self, auth_key: dict[str, Any]) -> None:
        values = self._auth_key_values(auth_key)
        if not values[0] or not values[3]:
            raise ValueError("auth key id and key_hash are required")
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO auth_keys
                   (key_id, name, role, key_hash, enabled, web_login_enabled,
                    created_at, last_used_at, extra_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(key_id) DO UPDATE SET
                     name=excluded.name,
                     role=excluded.role,
                     key_hash=excluded.key_hash,
                     enabled=excluded.enabled,
                     web_login_enabled=excluded.web_login_enabled,
                     created_at=excluded.created_at,
                     last_used_at=excluded.last_used_at,
                     extra_json=excluded.extra_json""",
                values,
            )

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        valid = [item for item in auth_keys if isinstance(item, dict)]
        key_ids = {str(item.get("id") or "").strip() for item in valid}
        key_ids.discard("")
        with self.transaction() as connection:
            for item in valid:
                if str(item.get("id") or "").strip() and str(item.get("key_hash") or "").strip():
                    values = self._auth_key_values(item)
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
                        values,
                    )
            if not key_ids:
                connection.execute("DELETE FROM auth_keys")
            else:
                placeholders = ",".join("?" for _ in key_ids)
                connection.execute(
                    f"DELETE FROM auth_keys WHERE key_id NOT IN ({placeholders})",
                    tuple(key_ids),
                )

    def delete_auth_key(self, key_id: str) -> None:
        with self.transaction() as connection:
            connection.execute("DELETE FROM auth_keys WHERE key_id = ?", (str(key_id),))

    def save_token_alias(
        self,
        alias_token: str,
        current_token: str,
        *,
        expires_at: float,
        last_used_at: float | None = None,
    ) -> None:
        with self.transaction() as connection:
            account_id = self._find_account_id(connection, current_token)
            connection.execute(
                """INSERT INTO account_token_aliases
                   (alias_token, account_id, current_token, expires_at, last_used_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(alias_token) DO UPDATE SET
                     account_id=excluded.account_id,
                     current_token=excluded.current_token,
                     expires_at=excluded.expires_at,
                     last_used_at=excluded.last_used_at""",
                (alias_token, account_id, current_token, expires_at, last_used_at),
            )

    def cleanup_token_aliases(self, now: float | None = None) -> int:
        current = time.time() if now is None else float(now)
        with self.transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM account_token_aliases WHERE expires_at <= ?",
                (current,),
            )
            return int(cursor.rowcount or 0)

    def load_token_aliases(self, now: float | None = None) -> list[dict[str, object]]:
        self._ensure_initialized()
        current = time.time() if now is None else float(now)
        rows = self._connection().execute(
            """SELECT alias_token, current_token, expires_at, last_used_at
               FROM account_token_aliases
               WHERE expires_at > ?
               ORDER BY expires_at DESC""",
            (current,),
        ).fetchall()
        return [
            {
                "alias_token": row["alias_token"],
                "current_token": row["current_token"],
                "expires_at": float(row["expires_at"]),
                "last_used_at": row["last_used_at"],
            }
            for row in rows
        ]

    def delete_token_alias(self, alias_token: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "DELETE FROM account_token_aliases WHERE alias_token = ?",
                (str(alias_token),),
            )

    def health_check(self) -> dict[str, Any]:
        try:
            self._ensure_initialized()
            connection = self._connection()
            connection.execute("SELECT 1").fetchone()
            return {
                "status": "healthy",
                "backend": "sqlite",
                "database_path": str(self.db_path),
                "account_count": int(connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]),
                "auth_key_count": int(connection.execute("SELECT COUNT(*) FROM auth_keys").fetchone()[0]),
            }
        except Exception as exc:
            return {"status": "unhealthy", "backend": "sqlite", "error": str(exc)}

    def get_backend_info(self) -> dict[str, Any]:
        return {
            "type": "sqlite",
            "description": "normalized SQLite application state",
            "database_path": str(self.db_path),
        }

    def close_current_thread(self) -> None:
        self._connection_registry.close_current_thread()

    def reap_dead_threads(self) -> int:
        return self._connection_registry.reap_dead_threads()

    def connection_count(self) -> int:
        return self._connection_registry.connection_count()

    def close(self) -> None:
        with self._initialize_lock:
            self._connection_registry.close()
