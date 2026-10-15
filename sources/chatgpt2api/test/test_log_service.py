from __future__ import annotations

import asyncio
import unittest
import tempfile
from threading import Event
from pathlib import Path
from unittest import mock

from services.log_service import LogService, LoggedCall
from services.quota_service import QuotaAccounting, QuotaReservation, count_image_outputs
from services.scheduler.errors import QueueFullError


class LoggedCallTests(unittest.TestCase):
    def test_dict_response_notifies_success_once(self):
        successes = []
        failures = []
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/chat/completions",
            model="test-model",
            summary="文本生成",
        )

        with mock.patch("services.log_service.log_service.add"):
            result = asyncio.run(
                logged_call.run(
                    lambda: {"ok": True},
                    on_success=lambda value: successes.append(value),
                    on_failure=lambda: failures.append(True),
                )
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(successes, [{"ok": True}])
        self.assertEqual(failures, [])

    def test_handled_http_error_notifies_failure_once(self):
        successes = []
        failures = []
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/chat/completions",
            model="test-model",
            summary="文本生成",
        )

        def handler():
            raise ValueError("upstream failed")

        with mock.patch("services.log_service.log_service.add"):
            response = asyncio.run(
                logged_call.run(
                    handler,
                    on_success=lambda value: successes.append(value),
                    on_failure=lambda: failures.append(True),
                )
            )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(successes, [])
        self.assertEqual(failures, [True])

    def test_async_stream_notifies_success_after_full_consumption(self):
        successes = []
        failures = []
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/chat/completions",
            model="test-model",
            summary="文本生成",
        )

        async def handler():
            yield {"delta": "one"}
            yield {"delta": "two"}

        with mock.patch("services.log_service.log_service.add"):
            response = asyncio.run(
                logged_call.run(
                    handler,
                    on_success=lambda value: successes.append(value),
                    on_failure=lambda: failures.append(True),
                )
            )
            self.assertEqual(successes, [])
            body = asyncio.run(self._consume_async_iterator(response.body_iterator))

        self.assertIn("data:", "".join(body))
        self.assertEqual(len(successes), 1)
        self.assertEqual(failures, [])

    def test_async_stream_close_notifies_failure_without_success(self):
        successes = []
        failures = []
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/chat/completions",
            model="test-model",
            summary="文本生成",
        )

        async def handler():
            yield {"delta": "one"}
            await asyncio.sleep(1)
            yield {"delta": "two"}

        with mock.patch("services.log_service.log_service.add"):
            response = asyncio.run(
                logged_call.run(
                    handler,
                    on_success=lambda value: successes.append(value),
                    on_failure=lambda: failures.append(True),
                )
            )

            async def consume_first_and_close():
                await response.body_iterator.__anext__()
                await response.body_iterator.__anext__()
                await response.body_iterator.aclose()

            asyncio.run(consume_first_and_close())

        self.assertEqual(successes, [])
        self.assertEqual(failures, [True])

    @staticmethod
    async def _consume_async_iterator(iterator):
        return [item async for item in iterator]

    def test_dict_response_strips_scheduler_internal_fields(self):
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/images/generations",
            model="gpt-image-2",
            summary="文生图",
        )

        with mock.patch("services.log_service.log_service.add"):
            result = asyncio.run(
                logged_call.run(
                    lambda: {
                        "data": [{"url": "https://files.example.test/image.png"}],
                        "_account_email": "account@example.test",
                        "_conversation_id": "conversation-1",
                    }
                )
            )

        self.assertNotIn("_account_email", result)
        self.assertNotIn("_conversation_id", result)

    def test_stream_scheduler_error_keeps_status_and_retry_after(self):
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/chat/completions",
            model="test-model",
            summary="文本生成",
        )

        async def runner(_handler, *_args, streaming=False):
            self.assertTrue(streaming)

            async def stream():
                raise QueueFullError("scheduler queue is full", retry_after_seconds=7)
                yield None

            return stream()

        with mock.patch("services.log_service.log_service.add"):
            response = asyncio.run(
                logged_call.run(lambda: None, scheduler_runner=runner, streaming=True)
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers["Retry-After"], "7")

    @mock.patch("services.log_service.log_service.add")
    def test_inline_data_urls_are_not_written_to_call_logs(self, add_log):
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/images/generations",
            model="gpt-image-2",
            summary="文生图",
        )

        logged_call.log(
            "调用完成",
            {
                "data": [
                    {"url": "data:image/png;base64,c2VjcmV0"},
                    {"url": "https://files.example.test/image.png"},
                ]
            },
        )

        detail = add_log.call_args.args[2]
        self.assertEqual(detail["urls"], ["https://files.example.test/image.png"])

    @mock.patch("services.log_service.log_service.add")
    def test_logged_call_omits_request_text_and_response_payload(self, add_log):
        logged_call = LoggedCall(
            identity={"id": "owner-1"},
            endpoint="/v1/chat/completions",
            model="test-model",
            summary="文本生成",
            request_text="secret prompt that must not be stored",
            request_shape={"input_image_parts": 1},
        )

        logged_call.log(
            "调用完成",
            {
                "status": 200,
                "request_text": "secret",
                "data": "large response payload",
                "choices": [{"message": {"content": "generated text"}}],
            },
        )

        detail = add_log.call_args.args[2]
        self.assertNotIn("request_text", detail)
        self.assertNotIn("data", detail)
        self.assertNotIn("choices", detail)
        self.assertEqual(detail["request_shape"], {"input_image_parts": 1})


class QuotaAccountingTests(unittest.TestCase):
    def test_count_image_outputs_handles_direct_response_partial_results(self):
        self.assertEqual(
            count_image_outputs({"data": [{"b64_json": "a"}, {"url": "https://x/2"}]}),
            2,
        )
        self.assertEqual(count_image_outputs({"data": [{"b64_json": "a"}]}), 1)

    def test_count_image_outputs_handles_responses_and_chat_shapes(self):
        self.assertEqual(
            count_image_outputs({"output": [{"type": "image_generation_call"}]}),
            1,
        )
        self.assertEqual(
            count_image_outputs(
                {"choices": [{"message": {"content": "![image](data:image/png;base64,abc)"}}]}
            ),
            1,
        )

    def test_stream_accounting_counts_partial_image_chunks(self):
        class FakeQuotaService:
            def __init__(self):
                self.committed = []
                self.released = 0

            def commit(self, reservation_id, units):
                self.committed.append((reservation_id, units))
                return True

            def release(self, reservation_id):
                self.released += 1
                return True

        service = FakeQuotaService()
        accounting = QuotaAccounting(
            QuotaReservation("reservation-1", "user-1", "image", 4),
            "image",
            service=service,
        )
        accounting.observe({"object": "image.generation.result", "data": [{"b64_json": "a"}]})
        accounting.observe(
            {
                "object": "image.generation.result",
                "data": [{"b64_json": "b"}, {"b64_json": "c"}],
            }
        )
        accounting.commit_success()
        self.assertEqual(service.committed, [("reservation-1", 3)])
        self.assertEqual(service.released, 0)


class LogServiceTests(unittest.TestCase):
    def test_sqlite_facade_supports_query_list_and_delete(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            service = LogService(root / "logs.db", root / "logs.jsonl")
            service.initialize()

            item = service.add("call", "stored", {"status": 200})
            result = service.query(page=1, page_size=20)

            self.assertEqual(result["total"], 1)
            self.assertEqual(result["items"][0]["id"], item["id"])
            self.assertEqual(result["items"][0]["detail"], {"status": 200})
            self.assertEqual(service.list(type="call", limit=1)[0]["id"], item["id"])
            self.assertEqual(service.delete([item["id"]]), 1)
            self.assertEqual(service.query(page=1, page_size=20)["total"], 0)

    def test_add_drops_request_and_response_payload_fields_before_storage(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            service = LogService(root / "logs.db", root / "logs.jsonl")

            item = service.add(
                "call",
                "payload policy",
                {
                    "request_text": "secret prompt",
                    "data": "large response",
                    "status": "success",
                },
            )

            self.assertIsNotNone(item)
            stored = service.query(page=1, page_size=20)["items"][0]["detail"]
            self.assertEqual(stored, {"status": "success"})

    def test_retention_scheduler_runs_and_stops(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            service = LogService(root / "logs.db", root / "logs.jsonl")
            stopped = Event()
            ran = Event()

            def cleanup(_retention_days: int) -> int:
                ran.set()
                return 0

            with mock.patch.object(service, "cleanup_expired", side_effect=cleanup):
                thread = service.start_retention_scheduler(
                    stopped,
                    lambda: 30,
                    interval_seconds=0.01,
                )
                self.assertTrue(ran.wait(1))
                stopped.set()
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
