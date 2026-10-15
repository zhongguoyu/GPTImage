from __future__ import annotations

from services.config import config
from services.quota_service import QuotaService
from services.scheduler.service import SchedulerService
from services.storage.factory import create_storage_backend
from services.storage.json_storage import JSONStorageBackend
from services.storage.sqlite_state import SQLiteStateStore


class EmptyAccountService:
    def scheduler_snapshot(self):
        return []


def test_factory_defaults_to_normalized_sqlite(monkeypatch, tmp_path):
    monkeypatch.delenv("STORAGE_BACKEND", raising=False)

    backend = create_storage_backend(tmp_path)

    assert isinstance(backend, SQLiteStateStore)
    assert backend.db_path == tmp_path / "app.db"


def test_json_backend_remains_an_explicit_rollback_option(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_BACKEND", "json")

    backend = create_storage_backend(tmp_path)

    assert isinstance(backend, JSONStorageBackend)
    assert backend.file_path == tmp_path / "accounts.json"


def test_scheduler_default_path_uses_app_db(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_DB_PATH", str(tmp_path / "app.db"))
    service = SchedulerService(
        account_service_instance=EmptyAccountService(),
        settings=config.get_scheduler_settings(),
    )

    assert service.state_store.db_path == tmp_path / "app.db"


def test_quota_default_path_uses_app_db(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_DB_PATH", str(tmp_path / "app.db"))
    service = QuotaService()

    assert service.store.db_path == tmp_path / "app.db"


def test_scheduler_and_quota_follow_custom_state_db_path(monkeypatch, tmp_path):
    db_path = tmp_path / "custom-state.db"
    monkeypatch.setenv("STATE_DB_PATH", str(db_path))

    scheduler = SchedulerService(
        account_service_instance=EmptyAccountService(),
        settings=config.get_scheduler_settings(),
    )
    quota = QuotaService()

    assert scheduler.state_store.db_path == db_path
    assert quota.store.db_path == db_path
