from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from fastapi import HTTPException

from services.config import ConfigStore
from services.protocol import conversation
from services.scheduler.models import Lane
from services.scheduler.errors import QueueFullError
from services.scheduler.service import SchedulerService
from services.scheduler.state_store import SchedulerStateStore


class FakeAccountService:
    def __init__(self) -> None:
        self.accounts = [
            {
                "access_token": "token-a",
                "type": "Free",
                "status": "正常",
                "source_type": "web",
                "quota": 1,
            },
            {
                "access_token": "token-b",
                "type": "Free",
                "status": "正常",
                "source_type": "web",
                "quota": 1,
            },
        ]

    def scheduler_snapshot(self):
        return [dict(item) for item in self.accounts]

    def scheduler_state_generation(self):
        return 1

    def mark_text_used(self, _token):
        return None


def make_service(tmp_dir: str) -> SchedulerService:
    with tempfile.TemporaryDirectory() as config_dir:
        path = Path(config_dir) / "config.json"
        path.write_text('{"auth-key":"test-auth"}', encoding="utf-8")
        settings = ConfigStore(path).get_scheduler_settings()
    lanes = dict(settings.lanes)
    lanes[Lane.TEXT] = replace(
        lanes[Lane.TEXT],
        worker_count=1,
        global_concurrency=1,
        per_account_concurrency=1,
        queue_wait_timeout_ms=1000,
    )
    settings = replace(settings, lanes=lanes)
    return SchedulerService(
        FakeAccountService(),
        settings=settings,
        state_store=SchedulerStateStore(Path(tmp_dir) / "scheduler.db"),
        model_router=lambda _model: {"free"},
    )


class SchedulerTextIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_rejection_is_exposed_as_http_429(self) -> None:
        from services.log_service import LoggedCall

        call = LoggedCall({}, "/v1/chat/completions", "auto", "文本生成")

        async def reject(_handler, *_args, streaming=False):
            raise QueueFullError("scheduler queue is full")

        with mock.patch("services.log_service.log_service.add"):
            with self.assertRaises(HTTPException) as raised:
                await call.run(lambda: None, scheduler_runner=reject)

        self.assertEqual(raised.exception.status_code, 429)

    async def test_text_backend_uses_worker_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(tmp_dir)
            requirement = service.classify_text("pro-chat")
            backend = object()

            with mock.patch.object(conversation, "OpenAIBackendAPI", return_value=backend) as factory:
                result = await service.run_text(
                    requirement,
                    lambda _lease, _cancel: conversation.text_backend("pro-chat"),
                )

            self.assertIs(result, backend)
            factory.assert_called_once()
            self.assertIn(factory.call_args.kwargs["access_token"], {"token-a", "token-b"})

    async def test_invalid_token_before_first_delta_replaces_lease_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(tmp_dir)
            requirement = service.classify_text("auto")
            request = conversation.ConversationRequest(model="auto")
            seen: list[str] = []

            def fake_backend(*, access_token):
                return SimpleNamespace(access_token=access_token, close=lambda: None)

            def fake_events(backend, **_kwargs):
                seen.append(backend.access_token)
                if len(seen) == 1:
                    raise RuntimeError("token_invalidated")
                yield {"type": "conversation.delta", "delta": "ok"}

            with (
                mock.patch.object(conversation, "OpenAIBackendAPI", side_effect=fake_backend),
                mock.patch.object(conversation, "conversation_events", side_effect=fake_events),
            ):
                stream = await service.run_stream(
                    Lane.TEXT,
                    requirement,
                    lambda _lease, _cancel: conversation.stream_text_deltas(
                        conversation.text_backend("auto"),
                        request,
                    ),
                )
                result = [item async for item in stream]

            self.assertEqual(result, ["ok"])
            self.assertEqual(len(seen), 2)
            self.assertEqual(set(seen), {"token-a", "token-b"})

    async def test_partial_text_failure_is_not_replayed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(tmp_dir)
            requirement = service.classify_text("auto")
            request = conversation.ConversationRequest(model="auto")

            def fake_backend(*, access_token):
                return SimpleNamespace(access_token=access_token, close=lambda: None)

            def fake_events(_backend, **_kwargs):
                yield {"type": "conversation.delta", "delta": "partial"}
                raise RuntimeError("upstream disconnected")

            with (
                mock.patch.object(conversation, "OpenAIBackendAPI", side_effect=fake_backend),
                mock.patch.object(conversation, "conversation_events", side_effect=fake_events),
            ):
                stream = await service.run_stream(
                    Lane.TEXT,
                    requirement,
                    lambda _lease, _cancel: conversation.stream_text_deltas(
                        conversation.text_backend("auto"),
                        request,
                    ),
                )
                with self.assertRaisesRegex(RuntimeError, "upstream disconnected"):
                    [item async for item in stream]

            metrics = service.lane(Lane.TEXT).metrics_snapshot()
            self.assertEqual(metrics["active_jobs"], 0)


if __name__ == "__main__":
    unittest.main()
