from __future__ import annotations

import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
from api.errors import install_exception_handlers
import api.support as support_module
from services.quota_service import QuotaExceededError, QuotaReservation
from services.user_key_concurrency import UserKeyConcurrencyLimiter


USER_IDENTITY = {"id": "user-1", "name": "普通用户", "role": "user"}
AUTH_HEADERS = {"Authorization": "Bearer user-token"}


class FakeQuotaService:
    def __init__(self):
        self.reserved: list[tuple[str, int, str]] = []
        self.committed: list[int] = []
        self.released = 0
        self.reject = False

    def reserve(self, identity, quota_type, units, **kwargs):
        if self.reject:
            raise QuotaExceededError(quota_type, units, 0)
        reservation_id = f"reservation-{len(self.reserved) + 1}"
        self.reserved.append((quota_type, units, kwargs["endpoint"]))
        return QuotaReservation(reservation_id, identity["id"], quota_type, units)

    def commit(self, reservation_id, units):
        self.committed.append(units)
        return True

    def release(self, reservation_id):
        self.released += 1
        return True


class QuotaRequestIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.quota = FakeQuotaService()
        self.handler_calls = []
        self.patches = [
            mock.patch.object(ai_module, "require_identity", lambda _authorization: dict(USER_IDENTITY)),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module, "_scheduler_enabled", return_value=False),
            mock.patch.object(support_module, "quota_service", self.quota),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        app = FastAPI()
        app.include_router(ai_module.create_router())
        install_exception_handlers(app)
        self.client = TestClient(app)

    def test_text_success_commits_one(self):
        def handler(_payload):
            self.handler_calls.append(True)
            return {"id": "chat-1", "choices": [{"message": {"content": "ok"}}]}

        with mock.patch.object(ai_module.openai_v1_chat_complete, "handle", handler):
            response = self.client.post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "messages": [{"role": "user", "content": "hi"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.quota.reserved, [("text", 1, "/v1/chat/completions")])
        self.assertEqual(self.quota.committed, [1])
        self.assertEqual(self.quota.released, 0)
        self.assertEqual(len(self.handler_calls), 1)

    def test_text_handler_failure_releases_without_commit(self):
        def handler(_payload):
            self.handler_calls.append(True)
            raise ValueError("upstream failed")

        with mock.patch.object(ai_module.openai_search, "handle", handler):
            response = self.client.post(
                "/v1/search",
                headers=AUTH_HEADERS,
                json={"prompt": "hi"},
            )

        self.assertEqual(response.status_code, 502, response.text)
        self.assertEqual(self.quota.committed, [])
        self.assertEqual(self.quota.released, 1)
        self.assertEqual(len(self.handler_calls), 1)

    def test_image_partial_response_commits_returned_count(self):
        def handler(payload):
            self.handler_calls.append(payload)
            return {"created": 1, "data": [{"b64_json": "a"}, {"b64_json": "b"}, {"b64_json": "c"}]}

        with mock.patch.object(ai_module.openai_v1_image_generations, "handle", handler):
            response = self.client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"prompt": "four", "n": 4},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.quota.reserved, [("image", 4, "/v1/images/generations")])
        self.assertEqual(self.quota.committed, [3])
        self.assertEqual(self.quota.released, 0)
        self.assertEqual(len(self.handler_calls), 1)

    def test_quota_exhaustion_returns_429_without_handler_call(self):
        self.quota.reject = True

        def handler(_payload):
            self.handler_calls.append(True)
            return {"created": 1, "data": [{"b64_json": "a"}]}

        with mock.patch.object(ai_module.openai_v1_image_generations, "handle", handler):
            response = self.client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"prompt": "one", "n": 1},
            )

        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(self.handler_calls, [])
        self.assertEqual(self.quota.committed, [])
        self.assertEqual(self.quota.released, 0)

    def test_text_stream_commits_after_normal_end(self):
        async def stream_handler(_payload):
            yield {"choices": [{"delta": {"content": "one"}}]}
            yield {"choices": [{"delta": {"content": "two"}}]}

        with mock.patch.object(ai_module.openai_v1_chat_complete, "handle", stream_handler):
            response = self.client.post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "stream": True, "messages": [{"role": "user", "content": "hi"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("one", response.text)
        self.assertEqual(self.quota.committed, [1])
        self.assertEqual(self.quota.released, 0)

    def test_text_limit_returns_429_before_handler_and_releases_after_manual_lease(self):
        limiter = UserKeyConcurrencyLimiter(
            text_limit_getter=lambda: 1,
            image_limit_getter=lambda: 1,
        )
        with mock.patch.object(support_module, "user_key_concurrency", limiter):
            held = limiter.try_acquire(USER_IDENTITY, "text")
            self.assertIsNotNone(held)
            try:
                with mock.patch.object(ai_module.openai_search, "handle", return_value={"ok": True}) as handler:
                    response = self.client.post("/v1/search", headers=AUTH_HEADERS, json={"prompt": "hi"})
                self.assertEqual(response.status_code, 429, response.text)
                self.assertEqual(response.headers["retry-after"], "1")
                self.assertEqual(response.json()["error"]["code"], "user_key_concurrency_limit")
                self.assertEqual(self.quota.reserved, [])
                handler.assert_not_called()
            finally:
                held.release()

            with mock.patch.object(ai_module.openai_search, "handle", return_value={"ok": True}):
                response = self.client.post("/v1/search", headers=AUTH_HEADERS, json={"prompt": "hi"})
            self.assertEqual(response.status_code, 200, response.text)

    def test_streaming_text_releases_user_key_lease_after_response_consumes(self):
        limiter = UserKeyConcurrencyLimiter(
            text_limit_getter=lambda: 1,
            image_limit_getter=lambda: 1,
        )

        async def stream_handler(_payload):
            self.assertEqual(limiter.active_count(USER_IDENTITY, "text"), 1)
            yield {"choices": [{"delta": {"content": "one"}}]}

        with mock.patch.object(support_module, "user_key_concurrency", limiter):
            with mock.patch.object(ai_module.openai_v1_chat_complete, "handle", stream_handler):
                response = self.client.post(
                    "/v1/chat/completions",
                    headers=AUTH_HEADERS,
                    json={"model": "auto", "stream": True, "messages": [{"role": "user", "content": "hi"}]},
                )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(limiter.active_count(USER_IDENTITY, "text"), 0)

    def test_image_generation_uses_image_lease_and_returns_429_when_held(self):
        limiter = UserKeyConcurrencyLimiter(
            text_limit_getter=lambda: 4,
            image_limit_getter=lambda: 1,
        )
        with mock.patch.object(support_module, "user_key_concurrency", limiter):
            held = limiter.try_acquire(USER_IDENTITY, "image")
            self.assertIsNotNone(held)
            try:
                with mock.patch.object(ai_module.openai_v1_image_generations, "handle", return_value={"ok": True}) as handler:
                    response = self.client.post(
                        "/v1/images/generations",
                        headers=AUTH_HEADERS,
                        json={"prompt": "hi"},
                    )
                self.assertEqual(response.status_code, 429, response.text)
                self.assertEqual(response.headers["retry-after"], "1")
                self.assertEqual(response.json()["error"]["code"], "user_key_concurrency_limit")
                handler.assert_not_called()
            finally:
                held.release()


if __name__ == "__main__":
    unittest.main()
