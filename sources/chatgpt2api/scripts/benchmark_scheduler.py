#!/usr/bin/env python3
"""Offline scheduler benchmark; no upstream calls or account mutations."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.config import config
from services.scheduler.models import Lane
from services.scheduler.service import SchedulerService
from services.scheduler.state_store import SchedulerStateStore


class BenchmarkAccounts:
    def __init__(self, count: int) -> None:
        self.accounts = [
            {
                "access_token": f"benchmark-token-{index}",
                "type": "Free",
                "status": "正常",
                "source_type": "web",
                "quota": 100000,
            }
            for index in range(count)
        ]

    def scheduler_snapshot(self):
        return [dict(item) for item in self.accounts]

    def scheduler_state_generation(self):
        return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--accounts", type=int, default=200)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--lane", choices=[lane.value for lane in Lane], default=Lane.TEXT.value)
    parser.add_argument("--latency-ms", type=float, default=2.0)
    return parser.parse_args()


async def run(args: argparse.Namespace) -> dict[str, object]:
    lane = Lane(args.lane)
    with tempfile.TemporaryDirectory(prefix="scheduler-benchmark-") as tmp_dir:
        service = SchedulerService(
            BenchmarkAccounts(max(1, args.accounts)),
            settings=config.get_scheduler_settings(),
            state_store=SchedulerStateStore(Path(tmp_dir) / "scheduler.db"),
            model_router=lambda _model: {"free"},
        )
        await service.start()
        started = time.perf_counter()

        def work(_lease, _cancel):
            time.sleep(max(0.0, args.latency_ms) / 1000.0)
            return True

        if lane == Lane.TEXT:
            requirement = service.classify_text("auto")
            submit = lambda: service.run_text(requirement, work)
        else:
            requirement = (
                service.classify_image_edit({"model": "gpt-image-2", "n": 1})
                if lane == Lane.IMAGE_EDIT
                else service.classify_image_generation({"model": "gpt-image-2", "n": 1})
            )
            submit = lambda: service.run_image(requirement, work)

        results = await asyncio.gather(
            *(asyncio.create_task(submit()) for _ in range(max(0, args.requests))),
            return_exceptions=True,
        )
        elapsed = max(0.000001, time.perf_counter() - started)
        metrics = service.lane(lane).metrics_snapshot()
        queue_waits = sorted(float(value) for value in metrics["queue_wait_ms"])
        account_health = service.account_pool.snapshot_health()
        await service.stop()

    def percentile(values: list[float], fraction: float) -> float:
        if not values:
            return 0.0
        index = min(len(values) - 1, int(round((len(values) - 1) * fraction)))
        return round(values[index], 3)

    rejected = sum(1 for result in results if isinstance(result, BaseException))
    return {
        "accounts": args.accounts,
        "requests": args.requests,
        "lane": lane.value,
        "admitted": metrics["admitted"],
        "completed": metrics["completed"],
        "rejected": rejected,
        "throughput_per_sec": round(float(metrics["completed"]) / elapsed, 3),
        "elapsed_sec": round(elapsed, 3),
        "queue_wait_p50_ms": percentile(queue_waits, 0.50),
        "queue_wait_p95_ms": percentile(queue_waits, 0.95),
        "max_inflight": int(metrics["peak_active_cost"]),
        "lease_leaks": sum(1 for item in account_health if int(item["inflight"]) != 0),
        "selection_probes": service.account_pool.selection_probe_count,
    }


def main() -> None:
    args = parse_args()
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
