from __future__ import annotations

import base64
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.image_tasks as image_tasks_module
from services.quota_service import QuotaExceededError
from services.user_key_concurrency import UserKeyConcurrencyLimitError


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
PNG_BYTES = b"\x89PNG\r\n\x1a\n"
DATA_IMAGE_URL = f"data:image/png;base64,{base64.b64encode(PNG_BYTES).decode('ascii')}"


class FakeImageTaskService:
    def __init__(self):
        self.generation_calls = []
        self.edit_calls = []
        self.reject_quota = False
        self.reject_concurrency = False

    async def submit_generation_async(self, identity, **kwargs):
        if self.reject_concurrency:
            raise UserKeyConcurrencyLimitError("image")
        if self.reject_quota:
            raise QuotaExceededError("image", 1, 0)
        return self.submit_generation(identity, **kwargs)

    def submit_generation(self, identity, **kwargs):
        self.generation_calls.append((identity, kwargs))
        return {
            "id": kwargs["client_task_id"],
            "status": "success",
            "mode": "generate",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
            "data": [{"url": f"{kwargs['base_url']}/images/fake.png"}],
        }

    def submit_edit(self, identity, **kwargs):
        self.edit_calls.append((identity, kwargs))
        return {
            "id": kwargs["client_task_id"],
            "status": "queued",
            "mode": "edit",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
        }

    async def submit_edit_async(self, identity, **kwargs):
        if self.reject_quota:
            raise QuotaExceededError("image", 1, 0)
        return self.submit_edit(identity, **kwargs)

    def list_tasks(self, _identity, ids):
        return {
            "items": [
                {
                    "id": task_id,
                    "status": "success",
                    "mode": "generate",
                    "created_at": "2026-01-01 00:00:00",
                    "updated_at": "2026-01-01 00:00:00",
                    "data": [{"url": "http://testserver/images/fake.png"}],
                }
                for task_id in ids
                if task_id != "missing"
            ],
            "missing_ids": [task_id for task_id in ids if task_id == "missing"],
        }


class ImageTasksApiTests(unittest.TestCase):
    def setUp(self):
        self.fake_service = FakeImageTaskService()
        self.service_patcher = mock.patch.object(image_tasks_module, "image_task_service", self.fake_service)
        self.identity_patcher = mock.patch.object(
            image_tasks_module,
            "require_identity",
            lambda _authorization: {"id": "admin", "name": "Admin", "role": "admin"},
        )
        self.service_patcher.start()
        self.identity_patcher.start()
        self.addCleanup(self.service_patcher.stop)
        self.addCleanup(self.identity_patcher.stop)
        app = FastAPI()
        app.include_router(image_tasks_module.create_router())
        self.client = TestClient(app)

    def test_create_generation_task(self):
        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "task-1", "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["id"], "task-1")
        self.assertEqual(payload["status"], "success")
        self.assertEqual(len(self.fake_service.generation_calls), 1)

    def test_quota_exhaustion_returns_insufficient_quota(self):
        self.fake_service.reject_quota = True
        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "quota-task", "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.json()["detail"]["error"]["code"], "insufficient_quota")

    def test_concurrency_exhaustion_returns_retryable_429(self):
        self.fake_service.reject_concurrency = True
        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "concurrency-task", "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.headers["retry-after"], "1")
        self.assertEqual(response.json()["detail"]["error"]["code"], "user_key_concurrency_limit")

    def test_create_edit_task_accepts_multiple_images(self):
        """测试图片编辑任务接口支持多个上传图片。"""
        response = self.client.post(
            "/api/image-tasks/edits",
            headers=AUTH_HEADERS,
            data={"client_task_id": "edit-1", "prompt": "edit", "model": "gpt-image-2"},
            files=[
                ("image", ("one.png", b"one", "image/png")),
                ("image", ("two.png", b"two", "image/png")),
            ],
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["id"], "edit-1")
        self.assertEqual(len(self.fake_service.edit_calls), 1)
        images = self.fake_service.edit_calls[0][1]["images"]
        self.assertEqual(len(images), 2)

    def test_create_edit_task_accepts_image_url(self):
        """测试图片编辑任务接口支持表单 image_url 引用。"""
        response = self.client.post(
            "/api/image-tasks/edits",
            headers=AUTH_HEADERS,
            data={
                "client_task_id": "edit-url-1",
                "prompt": "edit",
                "model": "gpt-image-2",
                "image_url": DATA_IMAGE_URL,
            },
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(self.fake_service.edit_calls), 1)
        images = self.fake_service.edit_calls[0][1]["images"]
        self.assertEqual(images, [(PNG_BYTES, "image_url.png", "image/png")])

    def test_list_tasks_reports_missing_ids(self):
        response = self.client.get("/api/image-tasks?ids=task-1,missing", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual([item["id"] for item in payload["items"]], ["task-1"])
        self.assertEqual(payload["missing_ids"], ["missing"])


if __name__ == "__main__":
    unittest.main()
