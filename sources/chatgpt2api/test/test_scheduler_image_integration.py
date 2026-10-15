from __future__ import annotations

import asyncio
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

import api.ai as ai_module
from services.protocol import openai_v1_chat_complete
from services.protocol import openai_v1_image_generations
from services.protocol.conversation import ImageOutput
from services.config import ConfigStore
from services.scheduler.errors import QueueFullError
from services.scheduler.models import AccountRequirement, Lane
from services.scheduler.service import SchedulerService
from services.scheduler.state_store import SchedulerStateStore


class FakeAccountService:
    def __init__(self, count: int = 8) -> None:
        self.accounts = [
            {
                "access_token": f"token-{index}",
                "type": "Free",
                "status": "正常",
                "source_type": "web",
                "quota": 100,
            }
            for index in range(count)
        ]

    def scheduler_snapshot(self):
        return [dict(item) for item in self.accounts]

    def scheduler_state_generation(self):
        return 1


def make_service(tmp_dir: str) -> SchedulerService:
    config_path = Path(tmp_dir) / "config.json"
    config_path.write_text('{"auth-key":"test-auth"}', encoding="utf-8")
    settings = ConfigStore(config_path).get_scheduler_settings()
    lanes = dict(settings.lanes)
    lanes[Lane.IMAGE_GENERATION] = replace(
        lanes[Lane.IMAGE_GENERATION],
        worker_count=4,
        global_concurrency=4,
        per_account_concurrency=4,
        queue_max_items=16,
        queue_max_cost=16,
        queue_wait_timeout_ms=500,
    )
    lanes[Lane.IMAGE_EDIT] = replace(
        lanes[Lane.IMAGE_EDIT],
        worker_count=2,
        global_concurrency=2,
        per_account_concurrency=2,
        queue_max_items=8,
        queue_max_cost=8,
        queue_wait_timeout_ms=500,
    )
    settings = replace(settings, lanes=lanes, image_total_per_account_concurrency=4)
    return SchedulerService(
        FakeAccountService(),
        settings=settings,
        state_store=SchedulerStateStore(Path(tmp_dir) / "scheduler.db"),
        model_router=lambda _model: {"free"},
    )


async def wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition did not become true before timeout")


class SchedulerImageIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_chat_image_scheduler_body_drops_raw_inputs_after_build(self) -> None:
        body = {
            "model": "gpt-image-2",
            "stream": False,
            "messages": [{"role": "user", "content": "x" * 100_000}],
            "images": ["encoded-image-data"],
            "_image_model": "gpt-image-2",
            "_image_prompt": "draw a cat",
            "_image_input_tokens": 12,
        }

        compact = ai_module._compact_image_scheduler_body(
            openai_v1_chat_complete.handle,
            body,
        )

        self.assertIs(compact, body)
        self.assertEqual(
            body,
            {
                "model": "gpt-image-2",
                "stream": False,
                "_image_model": "gpt-image-2",
                "_image_prompt": "draw a cat",
                "_image_input_tokens": 12,
            },
        )

    async def test_generation_route_adapter_submits_each_output_unit(self) -> None:
        class FakeImageScheduler:
            def __init__(self) -> None:
                self.requirement = None
                self.indices: list[int] = []

            def classify_image_generation(self, payload):
                self.requirement = AccountRequirement(
                    Lane.IMAGE_GENERATION,
                    str(payload.get("model")),
                    cost=int(payload.get("n") or 1),
                )
                return self.requirement

            async def run_image_units(self, requirement, factory):
                return [factory(index) for index in range(1, requirement.cost + 1)]

        scheduler = FakeImageScheduler()
        payload = {"prompt": "cat", "model": "gpt-image-2", "n": 4, "response_format": "b64_json"}
        def fake_generate(request, index, total):
            scheduler.indices.append(index)
            return [
                ImageOutput(
                    kind="result",
                    model=request.model,
                    index=index,
                    total=total,
                    data=[{"b64_json": f"image-{index}"}],
                )
            ]

        with (
            mock.patch.object(ai_module, "scheduler_service", scheduler),
            mock.patch.object(ai_module, "generate_image_unit", side_effect=fake_generate),
        ):
            runner = ai_module._image_scheduler_runner(
                "gpt-image-2",
                payload,
                Lane.IMAGE_GENERATION,
            )
            result = await runner(openai_v1_image_generations.handle, payload)

        self.assertEqual(scheduler.requirement.cost, 4)
        self.assertEqual(scheduler.indices, [1, 2, 3, 4])
        self.assertEqual(len(result["data"]), 4)

    async def test_generation_n_four_uses_four_independent_units(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(tmp_dir)
            await service.start()
            release = threading.Event()
            started: list[int] = []

            def work(index: int) -> int:
                started.append(index)
                release.wait(2.0)
                return index

            requirement = service.classify_image_generation({"model": "gpt-image-2", "n": 4})
            task = asyncio.create_task(service.run_image_units(requirement, work))
            await wait_for(
                lambda: service.lane(Lane.IMAGE_GENERATION).metrics_snapshot()["active_cost"] == 4,
            )
            self.assertEqual(service.lane(Lane.IMAGE_GENERATION).metrics_snapshot()["active_jobs"], 4)
            self.assertEqual(len(started), 4)

            release.set()
            self.assertEqual(sorted(await task), [1, 2, 3, 4])
            await service.lane(Lane.IMAGE_GENERATION).wait_until_idle()
            self.assertEqual(service.lane(Lane.IMAGE_GENERATION).metrics_snapshot()["active_cost"], 0)
            self.assertTrue(all(item["inflight"] == 0 for item in service.account_pool.snapshot_health()))
            await service.stop()

    async def test_generation_batch_rejects_atomically_when_queue_is_full(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(tmp_dir)
            service.lane(Lane.IMAGE_GENERATION).settings = replace(
                service.lane(Lane.IMAGE_GENERATION).settings,
                queue_max_items=2,
            )
            await service.start()
            calls: list[int] = []
            requirement = service.classify_image_generation({"model": "gpt-image-2", "n": 4})

            with self.assertRaises(QueueFullError):
                await service.run_image_units(
                    requirement,
                    lambda index: calls.append(index),
                )
            self.assertEqual(calls, [])
            self.assertEqual(service.lane(Lane.IMAGE_GENERATION).metrics_snapshot()["active_jobs"], 0)
            await service.stop()

    async def test_edit_lane_can_progress_while_generation_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(tmp_dir)
            await service.start()
            release = threading.Event()
            generation_started = threading.Event()

            def generation_work(_lease, _cancel):
                generation_started.set()
                release.wait(2.0)
                return "generation"

            generation_requirement = service.classify_image_generation({"model": "gpt-image-2", "n": 1})
            generation_task = asyncio.create_task(
                service.run_image(generation_requirement, generation_work),
            )
            await wait_for(generation_started.is_set)

            edit_requirement = service.classify_image_edit({"model": "gpt-image-2", "n": 1})
            edit_result = await service.run_image(edit_requirement, lambda: "edit")
            self.assertEqual(edit_result, "edit")
            self.assertEqual(service.lane(Lane.IMAGE_EDIT).metrics_snapshot()["rejected"], 0)

            release.set()
            self.assertEqual(await generation_task, "generation")
            await service.stop()


if __name__ == "__main__":
    unittest.main()
