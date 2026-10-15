from __future__ import annotations

from services.auth_service import AuthService
from services.storage.sqlite_state import SQLiteStateStore


class IncrementalAuthStorage(SQLiteStateStore):
    def __init__(self, db_path):
        super().__init__(db_path, auto_migrate=False)
        self.snapshot_calls = 0
        self.single_calls = 0
        self.delete_calls = 0

    def save_auth_keys(self, auth_keys):
        self.snapshot_calls += 1
        return super().save_auth_keys(auth_keys)

    def save_auth_key(self, auth_key):
        self.single_calls += 1
        return super().save_auth_key(auth_key)

    def delete_auth_key(self, key_id):
        self.delete_calls += 1
        return super().delete_auth_key(key_id)


def test_auth_service_uses_incremental_user_key_writes(tmp_path):
    storage = IncrementalAuthStorage(tmp_path / "app.db")
    service = AuthService(storage)

    public, raw_key = service.create_key(role="user", name="client")
    updated = service.update_key(public["id"], {"web_login_enabled": False})
    assert updated is not None
    assert updated["web_login_enabled"] is False
    assert service.authenticate_web_login(raw_key) is None
    assert service.delete_key(public["id"]) is True

    assert storage.snapshot_calls == 0
    assert storage.single_calls == 2
    assert storage.delete_calls == 1
