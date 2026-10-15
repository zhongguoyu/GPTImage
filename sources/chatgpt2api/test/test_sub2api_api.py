from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from services.sub2api_service import Sub2APIConfig, Sub2APISyncBusyError


class FakeAutoSyncService:
    def __init__(self) -> None:
        self.notify_server_changed = mock.Mock()
        self.server_deleted = mock.Mock()


class Sub2APIApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.config = Sub2APIConfig(Path(self.temp_dir.name) / "sub2api.json")
        self.server = self.config.add_server(
            name="existing",
            base_url="https://existing.example.test",
            email="",
            password="",
            api_key="existing-key",
            group_id="",
        )
        self.fake_import = mock.Mock()
        self.fake_import.start_import.return_value = {
            "job_id": "job-1",
            "status": "pending",
            "total": 1,
        }
        self.fake_auto = FakeAutoSyncService()

        def require_admin(authorization):
            if authorization == "Bearer user-token":
                raise HTTPException(status_code=403, detail={"error": "admin required"})
            return {"id": "admin", "role": "admin"}

        self.patches = [
            mock.patch.object(accounts_module, "sub2api_config", self.config),
            mock.patch.object(accounts_module, "sub2api_import_service", self.fake_import),
            mock.patch.object(accounts_module, "sub2api_auto_sync_service", self.fake_auto, create=True),
            mock.patch.object(accounts_module, "require_admin", require_admin),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        app = FastAPI()
        app.include_router(accounts_module.create_router())
        self.client = TestClient(app)
        self.admin_headers = {"Authorization": "Bearer admin-token"}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_create_returns_auto_sync_configuration_and_notifies_immediately(self):
        response = self.client.post(
            "/api/sub2api/servers",
            headers=self.admin_headers,
            json={
                "name": "remote",
                "base_url": "https://sub2api.example.test",
                "api_key": "new-secret",
                "auto_sync_interval_minutes": 5,
            },
        )

        self.assertEqual(response.status_code, 200, response.text)
        server = response.json()["server"]
        self.assertEqual(server["auto_sync_interval_minutes"], 5)
        self.assertNotIn("password", server)
        self.assertNotIn("api_key", server)
        self.fake_auto.notify_server_changed.assert_called_once_with(
            server["id"], immediate=True,
        )

    def test_negative_interval_is_rejected(self):
        response = self.client.post(
            "/api/sub2api/servers",
            headers=self.admin_headers,
            json={
                "base_url": "https://sub2api.example.test",
                "api_key": "new-secret",
                "auto_sync_interval_minutes": -1,
            },
        )

        self.assertEqual(response.status_code, 422)

    def test_relevant_update_triggers_immediate_and_name_only_update_does_not(self):
        self.client.post(
            f"/api/sub2api/servers/{self.server['id']}",
            headers=self.admin_headers,
            json={"name": "renamed"},
        )
        self.fake_auto.notify_server_changed.assert_not_called()

        response = self.client.post(
            f"/api/sub2api/servers/{self.server['id']}",
            headers=self.admin_headers,
            json={"group_id": "group-1", "auto_sync_interval_minutes": 5},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.fake_auto.notify_server_changed.assert_called_once_with(
            self.server["id"], immediate=True,
        )

        self.fake_auto.notify_server_changed.reset_mock()
        disabled = self.client.post(
            f"/api/sub2api/servers/{self.server['id']}",
            headers=self.admin_headers,
            json={"auto_sync_interval_minutes": 0},
        )
        self.assertEqual(disabled.status_code, 200, disabled.text)
        self.fake_auto.notify_server_changed.assert_called_once_with(
            self.server["id"], immediate=False,
        )

    def test_busy_manual_import_returns_409(self):
        self.fake_import.start_import.side_effect = Sub2APISyncBusyError("busy")

        response = self.client.post(
            f"/api/sub2api/servers/{self.server['id']}/import",
            headers=self.admin_headers,
            json={"account_ids": ["remote-1"]},
        )

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["detail"]["code"], "sub2api_sync_busy")

    def test_delete_notifies_scheduler(self):
        response = self.client.delete(
            f"/api/sub2api/servers/{self.server['id']}",
            headers=self.admin_headers,
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.fake_auto.server_deleted.assert_called_once_with(self.server["id"])

    def test_non_admin_cannot_modify_connections(self):
        response = self.client.get(
            "/api/sub2api/servers",
            headers={"Authorization": "Bearer user-token"},
        )

        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
