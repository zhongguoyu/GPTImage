from __future__ import annotations

import json

from services.storage.base import StorageBackend
from services.storage.sqlite_state import SQLiteStateStore


class SnapshotBackend(StorageBackend):
    def __init__(self):
        self.accounts = []
        self.auth_keys = []

    def load_accounts(self):
        return list(self.accounts)

    def save_accounts(self, accounts):
        self.accounts = list(accounts)

    def load_auth_keys(self):
        return list(self.auth_keys)

    def save_auth_keys(self, auth_keys):
        self.auth_keys = list(auth_keys)

    def health_check(self):
        return {"status": "healthy"}

    def get_backend_info(self):
        return {"type": "test"}


def test_initialize_creates_normalized_state_tables(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)

    store.initialize()

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
    } <= store.table_names()


def test_connection_uses_wal_and_bounded_cache(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    store.initialize()

    assert str(store.scalar("PRAGMA journal_mode")).lower() == "wal"
    assert store.scalar("PRAGMA busy_timeout") == 5000
    assert store.scalar("PRAGMA cache_size") < 0


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

    loaded = store.load_accounts()[0]
    assert loaded["access_token"] == "token-a"
    assert loaded["refresh_token"] == "refresh-a"
    assert loaded["email"] == "a@example.com"
    assert loaded["limits_progress"] == [{"name": "text", "remaining": 42, "reset_after": 12}]
    assert loaded["custom_flag"] == "kept"
    extra_json = store.scalar("SELECT extra_json FROM accounts WHERE access_token = ?", ("token-a",))
    assert json.loads(str(extra_json)) == {"custom_flag": "kept"}


def test_save_account_updates_one_row_without_snapshot_delete(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db", auto_migrate=False)
    store.initialize()
    store.save_accounts([
        {"access_token": "token-a", "email": "a@example.com"},
        {"access_token": "token-b", "email": "b@example.com"},
    ])

    store.save_account({"access_token": "token-a", "email": "new@example.com"})

    rows = store.load_accounts()
    assert {row["access_token"] for row in rows} == {"token-a", "token-b"}
    assert next(row for row in rows if row["access_token"] == "token-a")["email"] == "new@example.com"


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


def test_legacy_storage_backend_gets_incremental_compatibility_defaults():
    backend = SnapshotBackend()
    backend.accounts = [{"access_token": "old"}, {"access_token": "keep"}]
    backend.auth_keys = [{"id": "old", "key_hash": "hash-old"}]

    backend.save_account({"access_token": "new"})
    backend.save_auth_key({"id": "new", "key_hash": "hash-new"})

    assert backend.accounts == [{"access_token": "old"}, {"access_token": "keep"}, {"access_token": "new"}]
    assert backend.auth_keys == [
        {"id": "old", "key_hash": "hash-old"},
        {"id": "new", "key_hash": "hash-new"},
    ]
