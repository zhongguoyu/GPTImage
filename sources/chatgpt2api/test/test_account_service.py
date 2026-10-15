from __future__ import annotations

from pathlib import Path

from services.account_service import AccountService
from services.storage.json_storage import JSONStorageBackend
from services.storage.sqlite_state import SQLiteStateStore


def make_service(tmp_path: Path) -> AccountService:
    return AccountService(JSONStorageBackend(tmp_path / "accounts.json"))


def test_finished_refresh_progress_keeps_summary_without_account_snapshot(tmp_path):
    service = make_service(tmp_path)
    service.init_refresh_progress("refresh-1", 2)

    service.finish_refresh_progress(
        "refresh-1",
        {
            "refreshed": 2,
            "errors": [],
            "items": [
                {"access_token": "secret-a", "email": "a@example.com"},
                {"access_token": "secret-b", "email": "b@example.com"},
            ],
        },
    )

    progress = service._refresh_progress["refresh-1"]
    assert progress is not None
    assert "items" not in progress["result"]
    assert progress["result"]["account_count"] == 2
    assert "secret-a" not in str(progress)


def test_expired_progress_and_token_aliases_are_removed(tmp_path):
    service = make_service(tmp_path)
    service.init_refresh_progress("expired", 1)
    service._refresh_progress["expired"]["updated_ts"] = 1.0
    service._token_aliases = {"old-token": "new-token"}
    service._token_alias_expiry = {"old-token": 1000.0}

    service.cleanup_runtime_metadata(now=2000.0)

    assert service.get_refresh_progress("expired") is None
    assert service._token_aliases == {}
    assert service._token_alias_expiry == {}


def test_rotated_token_alias_survives_sqlite_reload(tmp_path):
    store = SQLiteStateStore(tmp_path / "app.db")
    service = AccountService(store)
    service.add_accounts(["old-token"])

    assert service._apply_refreshed_tokens(
        "old-token",
        {"access_token": "new-token"},
        "test",
    ) == "new-token"
    assert service.resolve_access_token("old-token") == "new-token"

    store.close()
    reloaded_store = SQLiteStateStore(tmp_path / "app.db")
    reloaded = AccountService(reloaded_store)
    try:
        assert reloaded.resolve_access_token("old-token") == "new-token"
    finally:
        reloaded_store.close()


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
