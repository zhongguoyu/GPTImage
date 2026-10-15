from __future__ import annotations

import json
import threading
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import services.sub2api_service as sub2api_service
from services.sub2api_service import Sub2APIConfig, Sub2APIImportService, _fetch_access_tokens_for_accounts


def make_server(server_id: str = "server-1") -> dict[str, str]:
    return {
        "id": server_id,
        "name": "test",
        "base_url": "https://sub2api.example.test",
        "email": "admin@example.test",
        "password": "secret",
        "api_key": "",
        "group_id": "",
    }


class _FakeResponse:
    ok = True
    status_code = 200
    text = ""

    def __init__(self, account_ids: list[str]):
        self._account_ids = account_ids

    def json(self) -> dict:
        return {
            "code": 0,
            "data": {
                "accounts": [
                    {
                        "id": account_id,
                        "credentials": {"access_token": f"token-{account_id}"},
                    }
                    for account_id in self._account_ids
                ],
            },
        }


class _FakeSession:
    def __init__(self, calls: list[list[str]]):
        self.calls = calls

    def get(self, _url: str, *, params: dict, **_kwargs):
        account_ids = [item for item in params["ids"].split(",") if item]
        self.calls.append(account_ids)
        return _FakeResponse(account_ids)

    def close(self) -> None:
        pass


class Sub2APIConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.temp_path = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_old_config_defaults_to_disabled_auto_sync(self):
        path = self.temp_path / "sub2api.json"
        path.write_text(json.dumps([make_server()]), encoding="utf-8")

        config = Sub2APIConfig(path)
        server = config.get_server("server-1")

        self.assertEqual(server["auto_sync_interval_minutes"], 0)
        self.assertEqual(server["auto_sync"]["status"], "disabled")
        self.assertIsNone(server["auto_sync"]["next_run_at"])

    def test_add_and_update_round_trip_auto_sync_interval(self):
        config = Sub2APIConfig(self.temp_path / "sub2api.json")

        added = config.add_server(
            name="test",
            base_url="https://sub2api.example.test",
            email="admin@example.test",
            password="secret",
            api_key="",
            group_id="",
            auto_sync_interval_minutes=5,
        )
        updated = config.update_server(added["id"], {"auto_sync_interval_minutes": 10})

        self.assertEqual(added["auto_sync_interval_minutes"], 5)
        self.assertEqual(updated["auto_sync_interval_minutes"], 10)

    def test_set_auto_sync_state_persists_only_existing_server(self):
        config = Sub2APIConfig(self.temp_path / "sub2api.json")
        added = config.add_server(
            name="test",
            base_url="https://sub2api.example.test",
            email="admin@example.test",
            password="secret",
            api_key="",
            group_id="",
            auto_sync_interval_minutes=5,
        )

        result = config.set_auto_sync_state(added["id"], status="running", last_error="")

        self.assertEqual(result["auto_sync"]["status"], "running")
        self.assertIsNone(config.set_auto_sync_state("missing", status="running"))

    def test_invalid_auto_sync_intervals_normalize_to_zero(self):
        path = self.temp_path / "sub2api.json"
        path.write_text(
            json.dumps([
                {**make_server(str(index)), "auto_sync_interval_minutes": value}
                for index, value in enumerate([-1, 0, 1.5, "invalid", True], start=1)
            ]),
            encoding="utf-8",
        )

        servers = Sub2APIConfig(path).list_servers()

        self.assertEqual([server["auto_sync_interval_minutes"] for server in servers], [0, 0, 0, 0, 0])


class Sub2APITokenExportTests(unittest.TestCase):
    def test_export_batches_ids_to_avoid_unbounded_query_length(self):
        ids = [f"remote-{index}" for index in range(125)]
        calls: list[list[str]] = []
        session = _FakeSession(calls)
        server = {**make_server(), "api_key": "test-api-key"}

        with mock.patch("services.sub2api_service.Session", return_value=session):
            tokens, errors = _fetch_access_tokens_for_accounts(server, ids)

        self.assertEqual(len(calls), 2)
        self.assertEqual([len(batch) for batch in calls], [100, 25])
        self.assertEqual(tokens, [f"token-{account_id}" for account_id in ids])
        self.assertEqual(errors, [])


class Sub2APIImportServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.config = Sub2APIConfig(Path(self.temp_dir.name) / "sub2api.json")
        self.server = self.config.add_server(
            name="test",
            base_url="https://sub2api.example.test",
            email="",
            password="",
            api_key="test-api-key",
            group_id="",
        )
        self.import_service = Sub2APIImportService(self.config)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_second_import_for_same_server_is_rejected_while_first_runs(self):
        started = threading.Event()
        release = threading.Event()

        def fetch(_server, _ids):
            started.set()
            release.wait(timeout=2)
            return ["token-1"], []

        with (
            mock.patch("services.sub2api_service._fetch_access_tokens_for_accounts", side_effect=fetch),
            mock.patch("services.sub2api_service.account_service") as account_service_mock,
        ):
            account_service_mock.add_accounts.return_value = {"added": 1, "skipped": 0}
            account_service_mock.refresh_accounts.return_value = {"refreshed": 1}
            first = self.import_service.start_import(self.server, ["remote-1"])
            self.assertTrue(started.wait(timeout=1))
            busy_error = getattr(sub2api_service, "Sub2APISyncBusyError", RuntimeError)
            with self.assertRaises(busy_error):
                self.import_service.start_import(self.server, ["remote-2"])
            release.set()
            job = self.import_service.wait_for_job(
                self.server["id"], first["job_id"], timeout=2,
            )

        self.assertEqual(job["status"], "completed")

    def test_import_exception_marks_job_failed_and_releases_lock(self):
        with mock.patch(
            "services.sub2api_service._fetch_access_tokens_for_accounts",
            side_effect=RuntimeError("export unavailable"),
        ):
            first = self.import_service.start_import(self.server, ["remote-1"])
            job = self.import_service.wait_for_job(
                self.server["id"], first["job_id"], timeout=2,
            )

        self.assertEqual(job["status"], "failed")
        second = self.import_service.start_import(self.server, ["remote-2"])
        self.assertIsNotNone(second)

    def test_import_error_list_is_bounded_but_failed_count_is_preserved(self):
        errors = [{"name": str(index), "error": "missing access_token"} for index in range(100)]
        with mock.patch(
            "services.sub2api_service._fetch_access_tokens_for_accounts",
            return_value=([], errors),
        ):
            job = self.import_service.start_import(self.server, [str(index) for index in range(100)])
            result = self.import_service.wait_for_job(
                self.server["id"], job["job_id"], timeout=2,
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failed"], 100)
        self.assertLessEqual(len(result["errors"]), 50)


if __name__ == "__main__":
    unittest.main()
