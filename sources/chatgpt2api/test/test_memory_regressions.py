from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from services.config import ConfigStore, config
from services.protocol.chat_completion_cache import ChatCompletionCache
from services.scheduler.account_pool import AccountPool
from services.scheduler.models import AccountRequirement, Lane, Outcome


def _scheduler_settings(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text('{"auth-key":"test-auth"}', encoding="utf-8")
    settings = ConfigStore(config_path).get_scheduler_settings()
    lanes = dict(settings.lanes)
    lanes[Lane.TEXT] = replace(lanes[Lane.TEXT], per_account_concurrency=2)
    return replace(settings, lanes=lanes)


def test_account_pool_memory_does_not_grow_with_reuse(tmp_path):
    pool = AccountPool(
        [
            {
                "access_token": f"token-{index}",
                "type": "Free",
                "status": "正常",
                "source_type": "web",
                "quota": 1,
            }
            for index in range(4)
        ],
        _scheduler_settings(tmp_path),
        model_router=lambda _model: {"free", "pro", "team"},
    )
    requirement = AccountRequirement(Lane.TEXT, "auto", cost=1)

    for _ in range(10000):
        lease = pool.try_acquire(requirement, deadline=pool.clock() + 5)
        assert lease is not None
        pool.release(lease, outcome=Outcome.SUCCESS, latency_ms=1)

    assert pool.debug_heap_entry_count() < pool.account_count() * 32
    # Each account is indexed in several image wildcard buckets as well as the
    # text bucket, so the aggregate stale bound is per capability bucket.
    assert pool.debug_stale_heap_entry_count() <= pool.account_count() * 4


def test_response_cache_resident_bytes_are_bounded(monkeypatch):
    monkeypatch.setitem(
        config.data,
        "chat_completion_cache",
        {
            "enabled": True,
            "ttl_seconds": 60,
            "max_entries": 1000,
            "max_bytes": 1024,
        },
    )
    cache = ChatCompletionCache()

    for index in range(100):
        cache.get_or_compute_response(
            str(index),
            lambda: {"text": "x" * 200},
        )

    assert cache.current_bytes <= 1024


def test_dockerfile_limits_glibc_allocator_arenas():
    dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
    text = dockerfile.read_text(encoding="utf-8")

    assert "MALLOC_ARENA_MAX=2" in text
    assert "MALLOC_TRIM_THRESHOLD_=131072" in text
