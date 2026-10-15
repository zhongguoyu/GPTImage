from __future__ import annotations

from concurrent.futures import Future
import time

import pytest

from services.config import config
from services.editable_file_task_service import EditableFileTaskService
from services.scheduler.errors import QueueFullError
from services.scheduler.state_store import SchedulerStateStore


def test_editable_file_tasks_use_shared_normalized_task_table(tmp_path):
    store = SchedulerStateStore(tmp_path / "app.db")
    service = EditableFileTaskService(tmp_path / "editable_file_tasks.json", state_store=store)
    now = time.time()
    service._tasks["owner-1:file-1"] = {
        "id": "file-1",
        "owner_id": "owner-1",
        "status": "success",
        "kind": "ppt",
        "model": "gpt-4o",
        "created_at": "2026-09-09 00:00:00",
        "updated_at": "2026-09-09 00:00:01",
        "created_ts": now,
        "updated_ts": now,
        "result": {"zip_url": "/files/ppt/file-1.zip"},
    }

    service._save_locked()

    record = store.get_task("file:owner-1:file-1")
    assert record is not None
    assert record["mode"] == "editable_ppt"
    assert record["result"]["zip_url"] == "/files/ppt/file-1.zip"
    assert not (tmp_path / "editable_file_tasks.json").exists()

    reloaded = EditableFileTaskService(tmp_path / "editable_file_tasks.json", state_store=store)
    result = reloaded.list_tasks({"id": "owner-1"}, ["file-1"])
    assert result["missing_ids"] == []
    assert result["items"][0]["status"] == "success"


def test_editable_file_service_does_not_load_image_tasks_from_shared_table(tmp_path):
    store = SchedulerStateStore(tmp_path / "app.db")
    store.save_task({
        "task_key": "owner-1:image-1",
        "owner_id": "owner-1",
        "task_id": "image-1",
        "mode": "generate",
        "status": "success",
    })

    service = EditableFileTaskService(tmp_path / "editable_file_tasks.json", state_store=store)

    assert service.list_tasks({"id": "owner-1"}, ["image-1"])["items"] == []


def test_spooled_file_task_inputs_are_removed_after_release(tmp_path):
    service = EditableFileTaskService(tmp_path / "tasks.json", spool_dir=tmp_path / "spool")

    paths = service._spool_inputs(["a" * 10000, "b" * 10000])

    assert len(paths) == 2
    assert all(path.exists() for path in paths)
    service._release_spooled_inputs(paths)
    assert list((tmp_path / "spool").iterdir()) == []


def test_file_task_admission_is_bounded(tmp_path):
    service = EditableFileTaskService(
        tmp_path / "tasks.json",
        max_workers=1,
        queue_max_items=1,
    )
    assert service._admission.acquire(blocking=False)
    assert service._admission.acquire(blocking=False)
    with pytest.raises(QueueFullError):
        service._submit(
            {"id": "owner-1", "role": "admin"},
            client_task_id="rejected",
            kind="ppt",
            prompt="prompt",
            base64_images=[],
            base_url="",
        )
    service._admission.release()
    service._admission.release()
    service.shutdown(wait=False)


def test_shared_task_cleanup_prunes_in_memory_snapshot(tmp_path, monkeypatch):
    monkeypatch.setitem(config.data, "file_task_retention_days", 1)
    store = SchedulerStateStore(tmp_path / "app.db")
    service = EditableFileTaskService(tmp_path / "tasks.json", state_store=store)
    old_ts = time.time() - 2 * 24 * 60 * 60
    task = {
        "id": "old-file",
        "owner_id": "owner-1",
        "status": "success",
        "kind": "ppt",
        "model": "gpt-4o",
        "created_at": "2026-09-08 00:00:00",
        "updated_at": "2026-09-08 00:00:00",
        "created_ts": old_ts,
        "updated_ts": old_ts,
    }
    service._tasks["owner-1:old-file"] = task
    service._save_locked()

    result = service.list_tasks({"id": "owner-1"}, ["old-file"])

    assert result["items"] == []
    assert result["missing_ids"] == ["old-file"]
    assert store.get_task("file:owner-1:old-file") is None
    service.shutdown(wait=False)


def test_cancelled_file_task_is_marked_terminal_and_releases_admission(tmp_path):
    service = EditableFileTaskService(tmp_path / "tasks.json", max_workers=1, queue_max_items=1)
    key = "owner-1:cancelled"
    now = time.time()
    service._tasks[key] = {
        "id": "cancelled",
        "owner_id": "owner-1",
        "status": "queued",
        "kind": "ppt",
        "created_at": "2026-09-09 00:00:00",
        "updated_at": "2026-09-09 00:00:00",
        "created_ts": now,
        "updated_ts": now,
    }
    assert service._admission.acquire(blocking=False)
    service._admitted.add(key)
    future = Future()
    assert future.cancel()

    service._on_future_done(key, (), future)

    assert service._tasks[key]["status"] == "error"
    assert service._tasks[key]["error"] == "任务已取消"
    assert "ended_ts" in service._tasks[key]
    assert service._admission.acquire(blocking=False)
    service._admission.release()
    service.shutdown(wait=False)
