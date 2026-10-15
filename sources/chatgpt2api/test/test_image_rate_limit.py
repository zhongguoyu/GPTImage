from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.account_service import AccountService
from services.config import config
from services.openai_backend_api import (
    ImagePollTimeoutError,
    ImageRateLimitedError,
    OpenAIBackendAPI,
    _is_content_policy_error,
    _is_image_rate_limit_error,
)
from services.protocol.conversation import (
    ConversationRequest,
    ConversationState,
    ImageGenerationError,
    stream_image_outputs,
    update_conversation_state,
)
from services.storage.json_storage import JSONStorageBackend

RATE_LIMIT_TOOL_TEXT = (
    "You're generating images too quickly. To ensure the best experience for everyone, "
    "we have rate limits in place. Please wait for an hour before generating more images."
)


def _rate_limited_conversation() -> dict:
    return {
        "mapping": {
            "tool": {
                "message": {
                    "author": {"role": "tool"},
                    "create_time": 1,
                    "content": {"content_type": "text", "parts": [RATE_LIMIT_TOOL_TEXT]},
                }
            },
            "assistant": {
                "message": {
                    "author": {"role": "assistant"},
                    "create_time": 2,
                    "content": {"content_type": "text", "parts": ["由于我这边发生了错误，我未能生成图片。"]},
                }
            },
        }
    }


class FakeBackend(OpenAIBackendAPI):
    def __init__(self, conversations: list[dict]) -> None:
        self.conversations = conversations
        self.calls = 0

    def _get_conversation(self, conversation_id: str) -> dict:
        self.calls += 1
        index = min(self.calls - 1, len(self.conversations) - 1)
        return self.conversations[index]


class IsImageRateLimitErrorTest(unittest.TestCase):
    def test_matches_upstream_tool_text(self) -> None:
        self.assertTrue(_is_image_rate_limit_error(RATE_LIMIT_TOOL_TEXT))

    def test_ignores_other_errors(self) -> None:
        self.assertFalse(_is_image_rate_limit_error("由于我这边发生了错误，我未能生成图片。"))
        self.assertFalse(_is_image_rate_limit_error(""))
        self.assertFalse(_is_content_policy_error("normal text"))


class ConversationStateRateLimitFlagTest(unittest.TestCase):
    def test_raw_payload_sets_rate_limited_flag(self) -> None:
        state = ConversationState()
        payload = (
            '{"o":"append","p":"/message/content/parts/0",'
            '"v":"You\'re generating images too quickly. Please wait for an hour."}'
        )
        update_conversation_state(state, payload)
        self.assertTrue(state.rate_limited)

    def test_normal_payload_keeps_flag_false(self) -> None:
        state = ConversationState()
        update_conversation_state(state, '{"v":"书法，随机一个汉字"}')
        self.assertFalse(state.rate_limited)


class PollRateLimitDetectionTest(unittest.TestCase):
    def test_poll_raises_rate_limited_instead_of_timeout(self) -> None:
        backend = FakeBackend([_rate_limited_conversation()])
        with (
            mock.patch.dict(config.data, {"image_poll_initial_wait_secs": 0}),
            mock.patch("services.openai_backend_api.time.sleep", lambda _seconds: None),
        ):
            with self.assertRaises(ImageRateLimitedError) as ctx:
                backend._poll_image_results("conv-1", timeout_secs=30)
        self.assertEqual(ctx.exception.conversation_id, "conv-1")
        self.assertEqual(backend.calls, 1)

    def test_poll_prefers_image_ids_over_rate_limit_text(self) -> None:
        conversation = _rate_limited_conversation()
        tool_message = conversation["mapping"]["tool"]["message"]
        tool_message["metadata"] = {"async_task_type": "image_gen"}
        tool_message["content"] = {
            "content_type": "multimodal_text",
            "parts": [
                {"content_type": "image_asset_pointer", "asset_pointer": "file-service://file-one"},
                RATE_LIMIT_TOOL_TEXT,
            ],
        }
        backend = FakeBackend([conversation])
        with (
            mock.patch.dict(config.data, {
                "image_poll_initial_wait_secs": 0,
                "image_check_before_hit_enabled": False,
            }),
            mock.patch("services.openai_backend_api.time.sleep", lambda _seconds: None),
        ):
            file_ids, _sediment_ids = backend._poll_image_results("conv-1", timeout_secs=10)
        self.assertEqual(file_ids, ["file-one"])

    def test_poll_timeout_still_raises_timeout_for_silent_conversation(self) -> None:
        backend = FakeBackend([{"mapping": {}}])
        with (
            mock.patch.dict(config.data, {
                "image_poll_initial_wait_secs": 0,
                "image_poll_interval_secs": 0.5,
            }),
            mock.patch("services.openai_backend_api.time.sleep", lambda _seconds: None),
        ):
            with self.assertRaises(ImagePollTimeoutError):
                backend._poll_image_results("conv-1", timeout_secs=1)


class StreamImageOutputsRateLimitTest(unittest.TestCase):
    def test_stream_raises_rate_limited_before_poll(self) -> None:
        request = ConversationRequest(model="gpt-image-2", prompt="书法，随机两个汉字")
        final_event = {
            "type": "conversation.done",
            "text": "由于我这边发生了错误，我未能生成图片。",
            "conversation_id": "conv-rate-1",
            "file_ids": [],
            "sediment_ids": [],
            "blocked": False,
            "tool_invoked": True,
            "turn_use_case": "image gen",
            "rate_limited": True,
        }
        backend = FakeBackend([])
        with mock.patch(
            "services.protocol.conversation.conversation_events",
            return_value=iter([final_event]),
        ):
            with self.assertRaises(ImageRateLimitedError) as ctx:
                list(stream_image_outputs(backend, request, 1, 1))
        self.assertEqual(ctx.exception.conversation_id, "conv-rate-1")
        self.assertIsNotNone(ctx.exception.retry_after_secs)


class AccountRateLimitCooldownTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.service = AccountService(JSONStorageBackend(Path(tmp.name) / "accounts.json"))
        self.service.add_accounts(["token-a", "token-b"])
        for token in ("token-a", "token-b"):
            self.service.update_account(token, {"quota": 10, "status": "正常"})

    def test_marked_account_is_excluded_from_candidates(self) -> None:
        self.service.mark_image_rate_limited("token-a", 1200)

        candidates = self.service._list_ready_candidate_tokens()
        self.assertIn("token-b", candidates)
        self.assertNotIn("token-a", candidates)
        self.assertGreater(self.service.image_rate_limit_remaining("token-a"), 0)
        self.assertEqual(self.service.image_rate_limit_remaining("token-b"), 0)

    def test_cooldown_expires(self) -> None:
        self.service.mark_image_rate_limited("token-a", 5)
        self.service._image_rate_limited_until["token-a"] = 0.0

        candidates = self.service._list_ready_candidate_tokens()
        self.assertIn("token-a", candidates)
        self.assertEqual(self.service.image_rate_limit_remaining("token-a"), 0)

    def test_cooldown_follows_token_rotation(self) -> None:
        self.service.mark_image_rate_limited("token-a", 1200)

        result = self.service._apply_refreshed_tokens("token-a", {"access_token": "token-a2"}, "test")

        self.assertEqual(result, "token-a2")
        self.assertIn("token-a2", self.service._image_rate_limited_until)
        self.assertNotIn("token-a", self.service._image_rate_limited_until)

    def test_snapshot_returns_remaining_and_skips_expired(self) -> None:
        self.service.mark_image_rate_limited("token-a", 1200)
        self.service.mark_image_rate_limited("token-b", 60)
        self.service._image_rate_limited_until["token-b"] = 0.0

        snapshot = self.service.image_rate_limit_snapshot()

        self.assertIn("token-a", snapshot)
        self.assertGreaterEqual(snapshot["token-a"], 1190)
        self.assertNotIn("token-b", snapshot)


class AttachImageRateLimitTest(unittest.TestCase):
    """GET /api/accounts 的冷却注入：legacy 内存表与调度器 breaker 取最大值。"""

    def test_attaches_max_remaining_from_legacy_and_breaker(self) -> None:
        import time as time_mod

        from api.accounts import _attach_image_rate_limit
        from utils.helper import anonymize_token

        items = [
            {"access_token": "token-a", "status": "正常"},
            {"access_token": "token-b", "status": "正常"},
            {"access_token": "token-c", "status": "正常"},
        ]
        with mock.patch("api.accounts.account_service") as fake_service, mock.patch(
            "api.accounts.scheduler_service"
        ) as fake_scheduler:
            fake_service.image_rate_limit_snapshot.return_value = {"token-a": 600.0}
            fake_scheduler.account_pool.snapshot_health.return_value = [
                {
                    "account_key": anonymize_token("token-b"),
                    "lane": "image_generation",
                    "breaker_state": "open",
                    "cooldown_until": time_mod.time() + 300,
                },
                {
                    "account_key": anonymize_token("token-b"),
                    "lane": "text",
                    "breaker_state": "open",
                    "cooldown_until": time_mod.time() + 9999,
                },
                {
                    "account_key": anonymize_token("token-a"),
                    "lane": "image_edit",
                    "breaker_state": "open",
                    "cooldown_until": time_mod.time() + 900,
                },
            ]
            _attach_image_rate_limit(items)

        self.assertEqual(items[0]["image_rate_limit_remaining_secs"], 900)
        self.assertEqual(items[1]["image_rate_limit_remaining_secs"], 300)
        self.assertNotIn("image_rate_limit_remaining_secs", items[2])

    def test_no_cooldown_state_leaves_items_untouched(self) -> None:
        from api.accounts import _attach_image_rate_limit

        items = [{"access_token": "token-a", "status": "正常"}]
        with mock.patch("api.accounts.account_service") as fake_service, mock.patch(
            "api.accounts.scheduler_service"
        ) as fake_scheduler:
            fake_service.image_rate_limit_snapshot.return_value = {}
            fake_scheduler.account_pool.snapshot_health.return_value = []
            _attach_image_rate_limit(items)

        self.assertNotIn("image_rate_limit_remaining_secs", items[0])


class RateLimitErrorShapeTest(unittest.TestCase):
    def test_image_generation_error_429_carries_retry_after(self) -> None:
        exc = ImageGenerationError(
            "rate limited",
            status_code=429,
            error_type="rate_limit_error",
            code="image_rate_limited",
            retry_after_secs=1200,
        )
        self.assertEqual(exc.status_code, 429)
        body = exc.to_openai_error()
        self.assertEqual(body["error"]["code"], "image_rate_limited")
        self.assertEqual(body["error"]["retry_after"], 1200)

    def test_rate_limited_error_defaults(self) -> None:
        exc = ImageRateLimitedError("limited", "conv-9", retry_after_secs=30.0)
        self.assertEqual(exc.conversation_id, "conv-9")
        self.assertEqual(exc.retry_after_secs, 30.0)


if __name__ == "__main__":
    unittest.main()
