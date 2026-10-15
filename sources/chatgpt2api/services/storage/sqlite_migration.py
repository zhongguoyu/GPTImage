from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from services.storage.sqlite_state import SQLiteStateStore


MIGRATION_VERSION = 1
_BLOCKED_KEYS = {
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


def _discover_sources(data_dir: Path) -> dict[str, Path]:
    root = Path(data_dir)
    return {
        "accounts": root / "accounts.json",
        "auth_keys": root / "auth_keys.json",
        "scheduler": root / "scheduler.db",
        "quota": root / "auth_quota.db",
        "image_tasks": root / "image_tasks.json",
        "editable_tasks": root / "editable_file_tasks.json",
    }


def _read_json_list(path: Path, *, wrapped: bool = False) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if wrapped and isinstance(value, dict):
        value = value.get("items")
    if isinstance(value, dict):
        value = value.get("tasks")
    return [item for item in value or [] if isinstance(item, dict)] if isinstance(value, list) else []


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone() is not None


def _source_rows(path: Path, table: str) -> list[sqlite3.Row]:
    if not path.exists():
        return []
    with closing(sqlite3.connect(str(path))) as connection:
        connection.row_factory = sqlite3.Row
        if not _table_exists(connection, table):
            return []
        return connection.execute(f"SELECT * FROM {table}").fetchall()


def _safe_json(value: object) -> object:
    if isinstance(value, dict):
        return {
            str(key): _safe_json(item)
            for key, item in value.items()
            if str(key).lower() not in _BLOCKED_KEYS
        }
    if isinstance(value, list):
        return [_safe_json(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _safe_json_text(value: object, default: str = "{}") -> str:
    try:
        return json.dumps(_safe_json(value), ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return default


def _timestamp(value: object, default: float | None = None) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value or "").strip()
    if raw:
        for candidate in (raw, raw.replace("Z", "+00:00")):
            try:
                return datetime.fromisoformat(candidate).timestamp()
            except (TypeError, ValueError, OverflowError):
                continue
    return time.time() if default is None else float(default)


def _image_result_metadata(value: object) -> dict[str, object] | None:
    if not isinstance(value, list):
        return None
    urls = [
        item.get("url")
        for item in value
        if isinstance(item, dict)
        and isinstance(item.get("url"), str)
        and item["url"].startswith(("http://", "https://"))
        and len(item["url"]) <= 4096
    ]
    return {"urls": urls, "count": len(value), "available": bool(urls)}


def _insert_auth_key(connection: sqlite3.Connection, store: SQLiteStateStore, item: dict[str, Any]) -> bool:
    if str(item.get("role") or "user").strip().lower() == "admin":
        return False
    values = store._auth_key_values(item)
    if not values[0] or not values[3]:
        return False
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
    return True


def _import_json_sources(
    connection: sqlite3.Connection,
    store: SQLiteStateStore,
    sources: dict[str, Path],
) -> dict[str, int]:
    accounts = _read_json_list(sources["accounts"])
    for item in accounts:
        token = str(item.get("access_token") or item.get("accessToken") or "").strip()
        if token:
            store._save_account_in_connection(connection, item)

    imported_auth_keys = 0
    for item in _read_json_list(sources["auth_keys"], wrapped=True):
        imported_auth_keys += int(_insert_auth_key(connection, store, item))

    imported_tasks = 0
    for item in _read_json_list(sources["image_tasks"]):
        task_id = str(item.get("id") or item.get("task_id") or "").strip()
        owner_id = str(item.get("owner_id") or "").strip()
        if not task_id or not owner_id:
            continue
        created_at = _timestamp(item.get("created_ts") or item.get("created_at"))
        updated_at = _timestamp(item.get("updated_ts") or item.get("updated_at"), created_at)
        metadata = {
            key: item[key]
            for key in ("size", "quality", "n", "account_ref", "created_at")
            if key in item
        }
        mode = "edit" if str(item.get("mode") or "").strip() == "edit" else "generate"
        connection.execute(
            """INSERT OR IGNORE INTO tasks
               (task_key, owner_id, task_id, task_type, status, model,
                remote_task_id, metadata_json, result_json, error, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                f"{owner_id}:{task_id}",
                owner_id,
                task_id,
                mode,
                str(item.get("status") or "error"),
                str(item.get("model") or "gpt-image-2"),
                str(item.get("conversation_id") or item.get("remote_task_id") or "").strip() or None,
                _safe_json_text(metadata),
                _safe_json_text(_image_result_metadata(item.get("data")), default="null")
                if item.get("data") is not None else None,
                str(item.get("error") or ""),
                created_at,
                updated_at,
            ),
        )
        imported_tasks += 1

    for item in _read_json_list(sources["editable_tasks"]):
        task_id = str(item.get("id") or "").strip()
        owner_id = str(item.get("owner_id") or "").strip()
        if not task_id or not owner_id:
            continue
        created_at = _timestamp(item.get("created_ts") or item.get("created_at"))
        updated_at = _timestamp(item.get("updated_ts") or item.get("updated_at"), created_at)
        task_key = f"file:{owner_id}:{task_id}"
        result = item.get("result")
        connection.execute(
            """INSERT OR IGNORE INTO tasks
               (task_key, owner_id, task_id, task_type, status, model,
                metadata_json, result_json, error, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task_key,
                owner_id,
                task_id,
                f"editable_{str(item.get('kind') or 'ppt')}",
                str(item.get("status") or "error"),
                str(item.get("model") or ""),
                _safe_json_text({}),
                _safe_json_text(result, default="null") if result is not None else None,
                str(item.get("error") or ""),
                created_at,
                updated_at,
            ),
        )
        imported_tasks += 1

    return {
        "accounts": sum(
            bool(str(item.get("access_token") or item.get("accessToken") or "").strip())
            for item in accounts
        ),
        "auth_keys": imported_auth_keys,
        "tasks": imported_tasks,
    }


def _import_scheduler_sources(
    connection: sqlite3.Connection,
    sources: dict[str, Path],
) -> dict[str, int]:
    health_rows = _source_rows(sources["scheduler"], "scheduler_account_health")
    for row in health_rows:
        connection.execute(
            """INSERT OR REPLACE INTO scheduler_account_health
               (account_key, lane, breaker_state, cooldown_until, quota_snapshot,
                ewma_latency_ms, recent_success_count, recent_failure_count,
                last_error_class, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            tuple(row[key] for key in (
                "account_key", "lane", "breaker_state", "cooldown_until",
                "quota_snapshot", "ewma_latency_ms", "recent_success_count",
                "recent_failure_count", "last_error_class", "updated_at",
            )),
        )

    task_rows = _source_rows(sources["scheduler"], "scheduler_tasks")
    for row in task_rows:
        updated_at = float(row["updated_at"] or time.time())
        connection.execute(
            """INSERT OR IGNORE INTO tasks
               (task_key, owner_id, task_id, task_type, status, model,
                remote_task_id, metadata_json, result_json, error, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(row["task_key"]),
                str(row["owner_id"] or ""),
                str(row["task_id"] or ""),
                str(row["mode"] or ""),
                str(row["status"] or ""),
                str(row["model"] or ""),
                row["remote_task_id"],
                _safe_json_text(_load_json(row["metadata_json"])),
                _safe_json_text(_load_json(row["result_json"]), default="null")
                if row["result_json"] is not None else None,
                str(row["error"] or ""),
                updated_at,
                updated_at,
            ),
        )

    return {"scheduler_health": len(health_rows), "scheduler_tasks": len(task_rows)}


def _import_quota_sources(
    connection: sqlite3.Connection,
    sources: dict[str, Path],
) -> dict[str, int]:
    quota_rows = _source_rows(sources["quota"], "key_quotas")
    for row in quota_rows:
        columns = (
            "key_id", "text_limit", "text_used", "text_reserved", "image_limit",
            "image_used", "image_reserved", "created_at", "updated_at", "deleted_at",
        )
        connection.execute(
            f"INSERT OR REPLACE INTO key_quotas ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            tuple(row[column] for column in columns),
        )

    reservation_rows = _source_rows(sources["quota"], "quota_reservations")
    for row in reservation_rows:
        columns = (
            "reservation_id", "key_id", "quota_type", "requested_units",
            "committed_units", "status", "request_id", "endpoint", "created_at",
            "expires_at", "settled_at",
        )
        connection.execute(
            f"INSERT OR REPLACE INTO quota_reservations ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            tuple(row[column] for column in columns),
        )

    return {"key_quotas": len(quota_rows), "quota_reservations": len(reservation_rows)}


def _load_json(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


def _source_count(sources: dict[str, Path], name: str) -> int:
    if name == "accounts":
        return len({
            str(item.get("access_token") or item.get("accessToken") or "").strip()
            for item in _read_json_list(sources["accounts"])
            if str(item.get("access_token") or item.get("accessToken") or "").strip()
        })
    if name == "auth_keys":
        return len({
            str(item.get("id") or "").strip()
            for item in _read_json_list(sources["auth_keys"], wrapped=True)
            if str(item.get("id") or "").strip()
            and str(item.get("key_hash") or "").strip()
            and str(item.get("role") or "user").strip().lower() != "admin"
        })
    if name == "tasks":
        scheduler = {
            str(row["task_key"] or "").strip()
            for row in _source_rows(sources["scheduler"], "scheduler_tasks")
            if str(row["task_key"] or "").strip()
        }
        image = {
            f"{str(item.get('owner_id') or '').strip()}:{str(item.get('id') or item.get('task_id') or '').strip()}"
            for item in _read_json_list(sources["image_tasks"])
            if str(item.get("id") or item.get("task_id") or "").strip()
            and str(item.get("owner_id") or "").strip()
        }
        editable = {
            f"file:{str(item.get('owner_id') or '').strip()}:{str(item.get('id') or '').strip()}"
            for item in _read_json_list(sources["editable_tasks"])
            if str(item.get("id") or "").strip() and str(item.get("owner_id") or "").strip()
        }
        return len(scheduler | image | editable)
    if name == "scheduler_health":
        return len({
            (str(row["account_key"] or "").strip(), str(row["lane"] or "").strip())
            for row in _source_rows(sources["scheduler"], "scheduler_account_health")
            if str(row["account_key"] or "").strip() and str(row["lane"] or "").strip()
        })
    if name == "key_quotas":
        return len({
            str(row["key_id"] or "").strip()
            for row in _source_rows(sources["quota"], "key_quotas")
            if str(row["key_id"] or "").strip()
        })
    if name == "quota_reservations":
        return len({
            str(row["reservation_id"] or "").strip()
            for row in _source_rows(sources["quota"], "quota_reservations")
            if str(row["reservation_id"] or "").strip()
        })
    return 0


def _source_identity_sets(sources: dict[str, Path]) -> dict[str, set[object]]:
    scheduler_task_keys = {
        str(row["task_key"] or "").strip()
        for row in _source_rows(sources["scheduler"], "scheduler_tasks")
        if str(row["task_key"] or "").strip()
    }
    image_task_keys = {
        f"{str(item.get('owner_id') or '').strip()}:{str(item.get('id') or item.get('task_id') or '').strip()}"
        for item in _read_json_list(sources["image_tasks"])
        if str(item.get("id") or item.get("task_id") or "").strip()
        and str(item.get("owner_id") or "").strip()
    }
    editable_task_keys = {
        f"file:{str(item.get('owner_id') or '').strip()}:{str(item.get('id') or '').strip()}"
        for item in _read_json_list(sources["editable_tasks"])
        if str(item.get("id") or "").strip() and str(item.get("owner_id") or "").strip()
    }
    return {
        "accounts": {
            str(item.get("access_token") or item.get("accessToken") or "").strip()
            for item in _read_json_list(sources["accounts"])
            if str(item.get("access_token") or item.get("accessToken") or "").strip()
        },
        "auth_keys": {
            str(item.get("id") or "").strip()
            for item in _read_json_list(sources["auth_keys"], wrapped=True)
            if str(item.get("id") or "").strip()
            and str(item.get("key_hash") or "").strip()
            and str(item.get("role") or "user").strip().lower() != "admin"
        },
        "tasks": scheduler_task_keys | image_task_keys | editable_task_keys,
        "scheduler_health": {
            (str(row["account_key"] or "").strip(), str(row["lane"] or "").strip())
            for row in _source_rows(sources["scheduler"], "scheduler_account_health")
            if str(row["account_key"] or "").strip() and str(row["lane"] or "").strip()
        },
        "key_quotas": {
            str(row["key_id"] or "").strip()
            for row in _source_rows(sources["quota"], "key_quotas")
            if str(row["key_id"] or "").strip()
        },
        "quota_reservations": {
            str(row["reservation_id"] or "").strip()
            for row in _source_rows(sources["quota"], "quota_reservations")
            if str(row["reservation_id"] or "").strip()
        },
    }


def _count_rows(connection: sqlite3.Connection, table: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _destination_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        name: _count_rows(connection, table)
        for name, table in (
            ("accounts", "accounts"),
            ("auth_keys", "auth_keys"),
            ("tasks", "tasks"),
            ("scheduler_health", "scheduler_account_health"),
            ("key_quotas", "key_quotas"),
            ("quota_reservations", "quota_reservations"),
        )
    }


def _destination_identity_sets(connection: sqlite3.Connection) -> dict[str, set[object]]:
    return {
        "accounts": {row[0] for row in connection.execute("SELECT access_token FROM accounts")},
        "auth_keys": {row[0] for row in connection.execute("SELECT key_id FROM auth_keys")},
        "tasks": {row[0] for row in connection.execute("SELECT task_key FROM tasks")},
        "scheduler_health": {
            (row[0], row[1])
            for row in connection.execute(
                "SELECT account_key, lane FROM scheduler_account_health"
            )
        },
        "key_quotas": {row[0] for row in connection.execute("SELECT key_id FROM key_quotas")},
        "quota_reservations": {
            row[0] for row in connection.execute("SELECT reservation_id FROM quota_reservations")
        },
    }


def _validate_imported_counts(
    connection: sqlite3.Connection,
    sources: dict[str, Path],
    imported: dict[str, int],
) -> None:
    expected = {
        "accounts": _source_count(sources, "accounts"),
        "auth_keys": _source_count(sources, "auth_keys"),
        "tasks": _source_count(sources, "tasks"),
        "scheduler_health": _source_count(sources, "scheduler_health"),
        "key_quotas": _source_count(sources, "key_quotas"),
        "quota_reservations": _source_count(sources, "quota_reservations"),
    }
    actual = _destination_counts(connection)
    expected_identities = _source_identity_sets(sources)
    actual_identities = _destination_identity_sets(connection)
    mismatches = {
        name: {"expected": expected[name], "actual": actual[name]}
        for name in expected
        if actual[name] < expected[name]
    }
    missing_identities = {
        name: sorted(expected_identities[name] - actual_identities[name], key=str)[:10]
        for name in expected_identities
        if expected_identities[name] - actual_identities[name]
    }
    if mismatches:
        raise ValueError(f"legacy state count mismatch: {mismatches}")
    if missing_identities:
        raise ValueError(f"legacy state identity mismatch: {missing_identities}")
    imported.update(actual)
    imported["verified"] = 1


def _fingerprint(sources: dict[str, Path]) -> str:
    digest = hashlib.sha256()
    for name in sorted(sources):
        path = sources[name]
        digest.update(name.encode("utf-8"))
        digest.update(str(path).encode("utf-8"))
        if not path.exists():
            digest.update(b"missing")
            continue
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def migrate_legacy_state(data_dir: Path, db_path: Path) -> dict[str, object]:
    sources = _discover_sources(Path(data_dir))
    store = SQLiteStateStore(Path(db_path), auto_migrate=False)
    store.initialize()
    try:
        if store.scalar(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (MIGRATION_VERSION,),
        ) is not None:
            return {"already_migrated": True}
        with store.transaction() as connection:
            imported = _import_json_sources(connection, store, sources)
            imported.update(_import_scheduler_sources(connection, sources))
            imported.update(_import_quota_sources(connection, sources))
            _validate_imported_counts(connection, sources, imported)
            connection.execute(
                """INSERT INTO schema_migrations
                   (version, source_fingerprint, verification_json, completed_at)
                   VALUES (?, ?, ?, ?)""",
                (
                    MIGRATION_VERSION,
                    _fingerprint(sources),
                    json.dumps(imported, ensure_ascii=False, sort_keys=True),
                    time.time(),
                ),
            )
            return imported
    finally:
        store.close()


def verify_sqlite_state(db_path: Path) -> dict[str, object]:
    path = Path(db_path)
    if not path.exists():
        return {"valid": False, "error": "database does not exist", "database_path": str(path)}
    store = SQLiteStateStore(path, auto_migrate=False)
    store.initialize()
    try:
        integrity = str(store.scalar("PRAGMA integrity_check") or "")
        counts = {
            table: _count_rows(store._connection(), table)
            for table in (
                "accounts",
                "auth_keys",
                "tasks",
                "key_quotas",
                "quota_reservations",
                "scheduler_account_health",
            )
        }
        marker = store._connection().execute(
            "SELECT version, verification_json, completed_at FROM schema_migrations ORDER BY version DESC LIMIT 1"
        ).fetchone()
        return {
            "valid": integrity.lower() == "ok",
            "integrity": integrity,
            "database_path": str(path),
            "counts": counts,
            "migration_version": marker[0] if marker is not None else None,
        }
    finally:
        store.close()
