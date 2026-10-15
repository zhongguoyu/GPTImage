from __future__ import annotations

import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from services.quota_service import QuotaLimitError


AUTH_HEADERS = {"Authorization": "Bearer admin-token"}


class FakeAuthService:
    def __init__(self):
        self.items = []
        self.next_id = 1

    def list_keys(self, role=None):
        return [dict(item) for item in self.items if role is None or item["role"] == role]

    def create_key(self, *, role, name=""):
        item = {
            "id": f"user-{self.next_id}",
            "name": name or "普通用户",
            "role": role,
            "enabled": True,
            "web_login_enabled": True,
            "created_at": "2026-09-08T00:00:00+00:00",
            "last_used_at": None,
        }
        self.next_id += 1
        self.items.append(item)
        return dict(item), f"sk-test-{item['id']}"

    def update_key(self, key_id, updates, *, role=None):
        for item in self.items:
            if item["id"] != key_id or (role is not None and item["role"] != role):
                continue
            item.update({key: value for key, value in updates.items() if key in {"name", "enabled", "web_login_enabled"}})
            return dict(item)
        return None

    def delete_key(self, key_id, *, role=None):
        before = len(self.items)
        self.items = [
            item
            for item in self.items
            if not (item["id"] == key_id and (role is None or item["role"] == role))
        ]
        return len(self.items) != before


class FakeQuotaService:
    def __init__(self):
        self.quotas = {}
        self.deleted = []
        self.fail_on_set_limits = False

    def ensure_key(self, key_id):
        self.quotas.setdefault(key_id, self._empty(key_id))

    def ensure_user_keys(self, items):
        for item in items:
            if item.get("role") == "user":
                self.ensure_key(item["id"])

    def _empty(self, key_id):
        return {
            "key_id": key_id,
            "text_limit": 0,
            "text_used": 0,
            "text_reserved": 0,
            "text_remaining": 0,
            "text_concurrency": 4,
            "image_limit": 0,
            "image_used": 0,
            "image_reserved": 0,
            "image_remaining": 0,
            "image_concurrency": 1,
            "created_at": 1,
            "updated_at": 1,
            "deleted_at": None,
        }

    def _refresh(self, quota):
        quota["text_remaining"] = max(0, quota["text_limit"] - quota["text_used"] - quota["text_reserved"])
        quota["image_remaining"] = max(0, quota["image_limit"] - quota["image_used"] - quota["image_reserved"])
        return quota

    def set_limits(
        self,
        key_id,
        *,
        text_unlimited=None,
        image_unlimited=None,
        text_limit=None,
        image_limit=None,
        text_concurrency=None,
        image_concurrency=None,
    ):
        if self.fail_on_set_limits:
            raise RuntimeError("quota store unavailable")
        self.ensure_key(key_id)
        quota = self.quotas[key_id]
        next_text = quota["text_limit"] if text_limit is None else text_limit
        next_image = quota["image_limit"] if image_limit is None else image_limit
        if next_text < quota["text_used"] + quota["text_reserved"]:
            raise QuotaLimitError()
        if next_image < quota["image_used"] + quota["image_reserved"]:
            raise QuotaLimitError()
        quota["text_limit"] = next_text
        quota["image_limit"] = next_image
        if text_concurrency is not None:
            quota["text_concurrency"] = text_concurrency
        if image_concurrency is not None:
            quota["image_concurrency"] = image_concurrency
        return self._refresh(quota)

    def list_user_quotas(self, items):
        self.ensure_user_keys(items)
        result = []
        for item in items:
            merged = dict(item)
            merged.update({key: value for key, value in self.quotas[item["id"]].items() if key != "key_id"})
            result.append(merged)
        return result

    def mark_deleted(self, key_id):
        self.deleted.append(key_id)
        self.ensure_key(key_id)
        self.quotas[key_id]["deleted_at"] = 2

    def get_quota(self, key_id):
        return self.quotas.get(key_id)


class QuotaApiTests(unittest.TestCase):
    def setUp(self):
        self.auth_service = FakeAuthService()
        self.quota_service = FakeQuotaService()
        self.patches = [
            mock.patch.object(accounts_module, "auth_service", self.auth_service),
            mock.patch.object(accounts_module, "quota_service", self.quota_service),
            mock.patch.object(accounts_module, "require_admin", lambda _authorization: {"id": "admin", "role": "admin"}),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        self.client = TestClient(app)

    def test_create_user_key_accepts_initial_limits(self):
        response = self.client.post(
            "/api/auth/users",
            headers=AUTH_HEADERS,
            json={
                "name": "设计组",
                "text_limit": 10,
                "image_limit": 4,
                "text_concurrency": 7,
                "image_concurrency": 3,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["item"]["text_limit"], 10)
        self.assertEqual(response.json()["item"]["image_limit"], 4)
        self.assertEqual(response.json()["item"]["text_concurrency"], 7)
        self.assertEqual(response.json()["item"]["image_concurrency"], 3)

    def test_update_user_key_changes_per_key_concurrency(self):
        created = self.client.post(
            "/api/auth/users",
            headers=AUTH_HEADERS,
            json={"name": "并发测试"},
        ).json()["item"]

        response = self.client.post(
            f"/api/auth/users/{created['id']}",
            headers=AUTH_HEADERS,
            json={"text_concurrency": 9, "image_concurrency": 4},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["item"]["text_concurrency"], 9)
        self.assertEqual(response.json()["item"]["image_concurrency"], 4)
        self.assertEqual(response.json()["items"][0]["text_concurrency"], 9)

    def test_update_user_key_changes_web_login_permission(self):
        created = self.client.post(
            "/api/auth/users",
            headers=AUTH_HEADERS,
            json={"name": "Web 登录测试"},
        ).json()["item"]
        self.assertTrue(created["web_login_enabled"])

        response = self.client.post(
            f"/api/auth/users/{created['id']}",
            headers=AUTH_HEADERS,
            json={"web_login_enabled": False},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["item"]["web_login_enabled"])
        self.assertFalse(response.json()["items"][0]["web_login_enabled"])

    def test_list_and_update_return_remaining_and_reject_low_limit(self):
        created = self.client.post(
            "/api/auth/users",
            headers=AUTH_HEADERS,
            json={"name": "设计组", "text_limit": 10, "image_limit": 4},
        ).json()["item"]
        quota = self.quota_service.quotas[created["id"]]
        quota["text_used"] = 8
        quota["text_reserved"] = 1
        self.quota_service._refresh(quota)

        listed = self.client.get("/api/auth/users", headers=AUTH_HEADERS)
        self.assertEqual(listed.status_code, 200, listed.text)
        self.assertEqual(listed.json()["items"][0]["text_remaining"], 1)

        response = self.client.post(
            f"/api/auth/users/{created['id']}",
            headers=AUTH_HEADERS,
            json={"text_limit": 8},
        )
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["detail"]["code"], "quota_limit_below_usage")

    def test_delete_marks_quota_history_deleted_after_auth_key_delete(self):
        created = self.client.post(
            "/api/auth/users",
            headers=AUTH_HEADERS,
            json={"name": "待删除", "text_limit": 1, "image_limit": 1},
        ).json()["item"]
        response = self.client.delete(f"/api/auth/users/{created['id']}", headers=AUTH_HEADERS)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.quota_service.deleted, [created["id"]])
        self.assertIsNotNone(self.quota_service.get_quota(created["id"]))

    def test_update_unknown_key_does_not_create_orphan_quota(self):
        response = self.client.post(
            "/api/auth/users/missing-user",
            headers=AUTH_HEADERS,
            json={"text_limit": 10},
        )

        self.assertEqual(response.status_code, 404, response.text)
        self.assertNotIn("missing-user", self.quota_service.quotas)

    def test_create_rolls_back_auth_key_when_quota_initialization_fails(self):
        self.quota_service.fail_on_set_limits = True

        response = self.client.post(
            "/api/auth/users",
            headers=AUTH_HEADERS,
            json={"name": "初始化失败"},
        )

        self.assertEqual(response.status_code, 500, response.text)
        self.assertEqual(self.auth_service.items, [])


if __name__ == "__main__":
    unittest.main()
