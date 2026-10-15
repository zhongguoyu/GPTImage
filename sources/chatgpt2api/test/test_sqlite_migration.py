from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from services.storage.sqlite_migration import migrate_legacy_state
from services.storage.sqlite_state import SQLiteStateStore


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def create_legacy_scheduler_db(path, *, task_key: str):
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE scheduler_account_health (
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
            CREATE TABLE scheduler_tasks (
                task_key TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                model TEXT NOT NULL,
                remote_task_id TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT,
                error TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL
            );
            INSERT INTO scheduler_account_health
                (account_key, lane, breaker_state, updated_at)
            VALUES ('token:one', 'text', 'closed', 10);
            """,
        )
        connection.execute(
            """INSERT INTO scheduler_tasks
               (task_key, owner_id, task_id, mode, status, model, metadata_json, updated_at)
               VALUES (?, 'user-1', 'task-1', 'image_generation', 'queued', 'gpt-image-2', '{}', 10)""",
            (task_key,),
        )
        connection.commit()
    finally:
        connection.close()


def create_legacy_quota_db(path, *, key_id: str):
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE key_quotas (
                key_id TEXT PRIMARY KEY,
                text_limit INTEGER NOT NULL DEFAULT 0,
                text_used INTEGER NOT NULL DEFAULT 0,
                text_reserved INTEGER NOT NULL DEFAULT 0,
                image_limit INTEGER NOT NULL DEFAULT 0,
                image_used INTEGER NOT NULL DEFAULT 0,
                image_reserved INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                deleted_at REAL
            );
            CREATE TABLE quota_reservations (
                reservation_id TEXT PRIMARY KEY,
                key_id TEXT NOT NULL,
                quota_type TEXT NOT NULL,
                requested_units INTEGER NOT NULL,
                committed_units INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                request_id TEXT,
                endpoint TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                settled_at REAL
            );
            """
        )
        connection.execute(
            """INSERT INTO key_quotas
               (key_id, text_limit, image_limit, created_at, updated_at)
               VALUES (?, 100, 20, 1, 1)""",
            (key_id,),
        )
        connection.execute(
            """INSERT INTO quota_reservations
               (reservation_id, key_id, quota_type, requested_units, status,
                created_at, expires_at)
               VALUES ('reservation-1', ?, 'image', 2, 'reserved', 1, 100)""",
            (key_id,),
        )
        connection.commit()
    finally:
        connection.close()


def count_rows(path, table):
    connection = sqlite3.connect(path)
    try:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        connection.close()


def has_migration_marker(path, version):
    connection = sqlite3.connect(path)
    try:
        return connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (version,),
        ).fetchone() is not None
    finally:
        connection.close()


def test_migration_imports_json_and_legacy_sqlite_once(tmp_path):
    write_json(tmp_path / "accounts.json", [{"access_token": "token-a", "email": "a@example.com"}])
    write_json(tmp_path / "auth_keys.json", {"items": [
        {
            "id": "user-1",
            "role": "user",
            "name": "one",
            "key_hash": "hash-1",
            "enabled": True,
            "web_login_enabled": False,
            "created_at": "2026-09-09T00:00:00+00:00",
            "last_used_at": None,
        }
    ]})
    create_legacy_scheduler_db(tmp_path / "scheduler.db", task_key="task-1")
    create_legacy_quota_db(tmp_path / "auth_quota.db", key_id="user-1")
    write_json(tmp_path / "image_tasks.json", {"tasks": [{
        "id": "image-1",
        "owner_id": "user-1",
        "status": "success",
        "mode": "generate",
        "model": "gpt-image-2",
        "data": [{"url": "https://files.example.test/image.png"}],
        "prompt": "must not be persisted",
    }]})

    first = migrate_legacy_state(tmp_path, tmp_path / "app.db")
    second = migrate_legacy_state(tmp_path, tmp_path / "app.db")

    assert first["accounts"] == 1
    assert first["auth_keys"] == 1
    assert second["already_migrated"] is True
    assert count_rows(tmp_path / "app.db", "accounts") == 1
    assert count_rows(tmp_path / "app.db", "auth_keys") == 1
    assert count_rows(tmp_path / "app.db", "tasks") == 2
    assert count_rows(tmp_path / "app.db", "key_quotas") == 1
    assert count_rows(tmp_path / "app.db", "quota_reservations") == 1
    assert count_rows(tmp_path / "app.db", "scheduler_account_health") == 1
    assert (tmp_path / "accounts.json").exists()
    assert (tmp_path / "auth_keys.json").exists()
    assert (tmp_path / "scheduler.db").exists()
    assert (tmp_path / "auth_quota.db").exists()
    assert (tmp_path / "image_tasks.json").exists()

    connection = sqlite3.connect(tmp_path / "app.db")
    try:
        result_json = connection.execute(
            "SELECT result_json FROM tasks WHERE task_id = 'image-1'"
        ).fetchone()[0]
        assert "must not be persisted" not in (result_json or "")
        assert "b64_json" not in (result_json or "")
        assert "https://files.example.test/image.png" in (result_json or "")
    finally:
        connection.close()


def test_migration_rolls_back_when_source_validation_fails(tmp_path, monkeypatch):
    write_json(tmp_path / "accounts.json", [{"access_token": "token-a"}])
    monkeypatch.setattr(
        "services.storage.sqlite_migration._validate_imported_counts",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("count mismatch")),
    )

    with pytest.raises(ValueError, match="count mismatch"):
        migrate_legacy_state(tmp_path, tmp_path / "app.db")

    assert count_rows(tmp_path / "app.db", "accounts") == 0
    assert not has_migration_marker(tmp_path / "app.db", 1)
    assert (tmp_path / "accounts.json").exists()


def test_migration_cli_verify_reports_valid_state(tmp_path):
    write_json(tmp_path / "accounts.json", [{"access_token": "token-a"}])
    migrate_legacy_state(tmp_path, tmp_path / "app.db")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/migrate_to_sqlite.py",
            "--data-dir",
            str(tmp_path),
            "--db",
            str(tmp_path / "app.db"),
            "--verify",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["valid"] is True
    assert payload["counts"]["accounts"] == 1


def test_migration_preserves_rows_already_present_in_shared_database(tmp_path):
    write_json(tmp_path / "accounts.json", [{"access_token": "token-new"}])
    existing_store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    existing_store.initialize()
    existing_store.save_account({"access_token": "token-existing", "email": "existing@example.com"})
    existing_store.close()

    result = migrate_legacy_state(tmp_path, tmp_path / "app.db")

    assert result["verified"] == 1
    assert result["accounts"] == 2
    assert count_rows(tmp_path / "app.db", "accounts") == 2


def test_migration_rejects_same_count_when_source_identity_is_missing(tmp_path, monkeypatch):
    write_json(tmp_path / "accounts.json", [{"access_token": "token-source"}])
    existing_store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    existing_store.initialize()
    existing_store.save_account({"access_token": "token-unrelated"})
    existing_store.close()

    monkeypatch.setattr(
        "services.storage.sqlite_migration._import_json_sources",
        lambda *args, **kwargs: {},
    )

    with pytest.raises(ValueError, match="identity mismatch"):
        migrate_legacy_state(tmp_path, tmp_path / "app.db")
