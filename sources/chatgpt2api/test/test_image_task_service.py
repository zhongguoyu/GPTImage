from __future__ import annotations

import base64
import json
import tempfile
from threading import Event
import time
import unittest
from pathlib import Path
from unittest import mock

from services.image_task_service import ImageTaskService, _collect_image_urls
from services.quota_service import AuthQuotaStore, QuotaExceededError, QuotaReservation, QuotaService
from services.scheduler.state_store import SchedulerStateStore
from services.storage.sqlite_migration import migrate_legacy_state
from services.user_key_concurrency import UserKeyConcurrencyLimitError, UserKeyConcurrencyLimiter


OWNER = {"id": "owner-1", "name": "Owner", "role": "admin"}
OTHER_OWNER = {"id": "owner-2", "name": "Other", "role": "user"}
USER_OWNER = {"id": "user-1", "name": "User", "role": "user"}


class FakeQuotaService:
    def __init__(self, *, reject: bool = False):
        self.reject = reject
        self.reservations = []
        self.commits = []
        self.releases = []

    def reserve(self, identity, quota_type, units, **kwargs):
        if self.reject:
            raise QuotaExceededError(quota_type, units, 0)
        reservation_id = f"image-reservation-{len(self.reservations) + 1}"
        self.reservations.append((reservation_id, identity["id"], quota_type, units, kwargs))
        return QuotaReservation(reservation_id, identity["id"], quota_type, units)

    def commit(self, reservation_id, units):
        self.commits.append((reservation_id, units))
        return True

    def release(self, reservation_id):
        self.releases.append(reservation_id)
        return True


def wait_for_task(service: ImageTaskService, identity: dict[str, object], task_id: str, status: str, timeout: float = 2.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        result = service.list_tasks(identity, [task_id])
        last = (result.get("items") or [None])[0]
        if last and last.get("status") == status:
            return last
        time.sleep(0.02)
    raise AssertionError(f"task {task_id} did not reach {status}, last={last}")


class ImageTaskServiceTests(unittest.TestCase):
    def make_service(
        self,
        path: Path,
        handler=None,
        quota_service=None,
        state_store=None,
        concurrency_limiter=None,
    ) -> ImageTaskService:
        return ImageTaskService(
            path,
            generation_handler=handler or (lambda _payload: {"data": [{"url": "http://example.test/image.png"}]}),
            edit_handler=handler or (lambda _payload: {"data": [{"url": "http://example.test/edit.png"}]}),
            retention_days_getter=lambda: 30,
            quota_service_instance=quota_service,
            state_store=state_store,
            concurrency_limiter=concurrency_limiter,
            spool_dir=path.parent / "image-task-spool",
        )

    def test_user_image_tasks_hold_one_image_lease_until_terminal_state(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            started = Event()
            release = Event()

            def handler(_payload):
                started.set()
                release.wait(timeout=2)
                return {"data": [{"url": "http://example.test/image.png"}]}

            limiter = UserKeyConcurrencyLimiter(
                text_limit_getter=lambda: 4,
                image_limit_getter=lambda: 1,
            )
            quota = FakeQuotaService()
            service = self.make_service(
                Path(tmp_dir) / "image_tasks.json",
                handler,
                quota_service=quota,
                concurrency_limiter=limiter,
            )
            try:
                service.submit_generation(
                    USER_OWNER,
                    client_task_id="first",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                )
                self.assertTrue(started.wait(timeout=2))
                self.assertEqual(limiter.active_count(USER_OWNER, "image"), 1)

                with self.assertRaises(UserKeyConcurrencyLimitError):
                    service.submit_generation(
                        USER_OWNER,
                        client_task_id="second",
                        prompt="cat",
                        model="gpt-image-2",
                        size=None,
                    )

                release.set()
                wait_for_task(service, USER_OWNER, "first", "success")
                self.assertEqual(limiter.active_count(USER_OWNER, "image"), 0)

                service.submit_generation(
                    USER_OWNER,
                    client_task_id="second",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                )
                wait_for_task(service, USER_OWNER, "second", "success")
                self.assertEqual(limiter.active_count(USER_OWNER, "image"), 0)
            finally:
                release.set()
                service.shutdown(wait=True)

    def test_duplicate_user_submit_does_not_acquire_second_image_lease(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            started = Event()
            release = Event()

            def handler(_payload):
                started.set()
                release.wait(timeout=2)
                return {"data": [{"url": "http://example.test/image.png"}]}

            limiter = UserKeyConcurrencyLimiter(
                text_limit_getter=lambda: 4,
                image_limit_getter=lambda: 1,
            )
            quota = FakeQuotaService()
            service = self.make_service(
                Path(tmp_dir) / "image_tasks.json",
                handler,
                quota_service=quota,
                concurrency_limiter=limiter,
            )
            try:
                first = service.submit_generation(
                    USER_OWNER,
                    client_task_id="duplicate-user-task",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                )
                self.assertTrue(started.wait(timeout=2))
                second = service.submit_generation(
                    USER_OWNER,
                    client_task_id="duplicate-user-task",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                )
                self.assertEqual(first["id"], second["id"])
                self.assertEqual(limiter.active_count(USER_OWNER, "image"), 1)
                release.set()
                wait_for_task(service, USER_OWNER, "duplicate-user-task", "success")
                self.assertEqual(limiter.active_count(USER_OWNER, "image"), 0)
            finally:
                release.set()
                service.shutdown(wait=True)

    def test_user_image_task_reserves_and_commits_successful_output(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = FakeQuotaService()
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", quota_service=quota)

            service.submit_generation(
                USER_OWNER,
                client_task_id="quota-success",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            task = wait_for_task(service, USER_OWNER, "quota-success", "success")
            self.assertEqual(len(task["data"]), 1)
            self.assertEqual([(item[1], item[2], item[3]) for item in quota.reservations], [("user-1", "image", 1)])
            self.assertEqual(quota.commits, [("image-reservation-1", 1)])
            self.assertEqual(quota.releases, [])

    def test_failed_user_image_task_releases_reservation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = FakeQuotaService()

            def handler(_payload):
                raise RuntimeError("upstream failed")

            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler, quota)
            service.submit_generation(
                USER_OWNER,
                client_task_id="quota-failure",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            wait_for_task(service, USER_OWNER, "quota-failure", "error")
            self.assertEqual(quota.commits, [])
            self.assertEqual(quota.releases, ["image-reservation-1"])

    def test_dispatch_failure_removes_task_and_releases_reservation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = FakeQuotaService()
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", quota_service=quota)

            def fail_dispatch(*_args, **_kwargs):
                raise RuntimeError("executor is closed")

            service._dispatch_task = fail_dispatch
            try:
                with self.assertRaisesRegex(RuntimeError, "executor is closed"):
                    service.submit_generation(
                        USER_OWNER,
                        client_task_id="dispatch-failure",
                        prompt="cat",
                        model="gpt-image-2",
                        size=None,
                    )
                self.assertEqual(service._tasks, {})
                self.assertEqual(quota.releases, ["image-reservation-1"])
            finally:
                service.shutdown(wait=False)

    def test_quota_exhaustion_rejects_image_task_before_persistence(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = FakeQuotaService(reject=True)
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", quota_service=quota)

            with self.assertRaises(QuotaExceededError):
                service.submit_generation(
                    USER_OWNER,
                    client_task_id="quota-rejected",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                    base_url="http://local.test",
                )

            self.assertEqual(service.list_tasks(USER_OWNER, ["quota-rejected"])["items"], [])

    def test_timeout_resume_reuses_persisted_reservation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = FakeQuotaService()
            limiter = UserKeyConcurrencyLimiter(
                text_limit_getter=lambda: 4,
                image_limit_getter=lambda: 1,
            )

            def handler(_payload):
                error = RuntimeError("生图超时")
                error.conversation_id = "conversation-quota"
                raise error

            service = self.make_service(
                Path(tmp_dir) / "image_tasks.json",
                handler,
                quota,
                concurrency_limiter=limiter,
            )
            service.submit_generation(
                USER_OWNER,
                client_task_id="quota-resume",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            task = wait_for_task(service, USER_OWNER, "quota-resume", "error")
            self.assertEqual(task["conversation_id"], "conversation-quota")
            self.assertEqual(len(quota.reservations), 1)
            self.assertEqual(quota.commits, [])
            self.assertEqual(quota.releases, [])
            self.assertEqual(limiter.active_count(USER_OWNER, "image"), 0)

            with mock.patch.object(service, "_dispatch_resume_poll") as dispatch:
                service.resume_poll(USER_OWNER, "quota-resume", 1.0)
            dispatch.assert_called_once()
            self.assertEqual(limiter.active_count(USER_OWNER, "image"), 1)

            backend = mock.Mock()
            backend._poll_image_results.return_value = (["file-1"], [])
            backend.resolve_conversation_image_urls.return_value = ["https://files.example.test/image.png"]
            with mock.patch("services.openai_backend_api.OpenAIBackendAPI", return_value=backend):
                service._run_resume_poll(
                    "user-1:quota-resume",
                    "conversation-quota",
                    1.0,
                    USER_OWNER,
                    "generate",
                    "gpt-image-2",
                    access_token="account-token",
                )

            self.assertEqual(len(quota.reservations), 1)
            self.assertEqual(quota.commits, [("image-reservation-1", 1)])
            self.assertEqual(quota.releases, [])
            self.assertEqual(limiter.active_count(USER_OWNER, "image"), 0)

    def test_quota_reservation_metadata_is_persisted_in_scheduler_store(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = FakeQuotaService()
            store = SchedulerStateStore(Path(tmp_dir) / "scheduler.db")
            service = self.make_service(
                Path(tmp_dir) / "image_tasks.json",
                quota_service=quota,
                state_store=store,
            )
            service.submit_generation(
                USER_OWNER,
                client_task_id="quota-persisted",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            wait_for_task(service, USER_OWNER, "quota-persisted", "success")
            record = store.get_task("user-1:quota-persisted")
            self.assertEqual(record["metadata"]["quota_reservation_id"], "image-reservation-1")
            self.assertEqual(record["metadata"]["quota_requested_units"], 1)

    def test_shared_migration_keeps_legacy_image_task_file_and_loads_only_image_tasks(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "image_tasks.json").write_text(
                '{"tasks":[{"id":"legacy-image","owner_id":"owner-1",'
                '"status":"success","mode":"generate","model":"gpt-image-2",'
                '"data":[{"url":"https://files.example.test/image.png"}]}]}',
                encoding="utf-8",
            )
            migrate_legacy_state(root, root / "app.db")
            store = SchedulerStateStore(root / "app.db")

            service = ImageTaskService(root / "image_tasks.json", state_store=store)

            self.assertTrue((root / "image_tasks.json").exists())
            self.assertEqual(
                service.list_tasks(OWNER, ["legacy-image"])["items"][0]["status"],
                "success",
            )
            self.assertEqual(store.list_tasks("owner-1", task_types=("editable_ppt",)), [])

    def test_shared_task_cleanup_uses_persisted_numeric_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            store = SchedulerStateStore(root / "app.db")
            old_ts = time.time() - 31 * 24 * 60 * 60
            store.save_task({
                "task_key": "owner-1:old-image",
                "owner_id": "owner-1",
                "task_id": "old-image",
                "mode": "generate",
                "status": "success",
                "model": "gpt-image-2",
                "result": {"urls": ["https://files.example.test/old.png"], "count": 1, "available": True},
                "created_at": old_ts,
                "updated_at": old_ts,
            })

            service = self.make_service(
                root / "image_tasks.json",
                state_store=store,
                handler=lambda _payload: {"data": [{"url": "https://example.test/new.png"}]},
            )

            assert service.list_tasks(OWNER, ["old-image"])["items"] == []
            assert store.get_task("owner-1:old-image") is None
            service.shutdown(wait=False)

    def test_shared_task_cleanup_reaches_rows_outside_loaded_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            store = SchedulerStateStore(root / "app.db")
            old_ts = time.time() - 31 * 24 * 60 * 60
            now = time.time()
            records = [{
                "task_key": "owner-1:old-image",
                "owner_id": "owner-1",
                "task_id": "old-image",
                "mode": "generate",
                "status": "success",
                "model": "gpt-image-2",
                "updated_at": old_ts,
            }]
            records.extend({
                "task_key": f"owner-1:current-{index}",
                "owner_id": "owner-1",
                "task_id": f"current-{index}",
                "mode": "generate",
                "status": "success",
                "model": "gpt-image-2",
                "created_at": now,
                "updated_at": now,
            } for index in range(1000))
            store.save_task_batch(records)

            service = self.make_service(root / "image_tasks.json", state_store=store)

            assert store.get_task("owner-1:old-image") is None
            assert len(store.list_tasks(task_types=("generate", "edit"), limit=2000)) == 1000
            service.shutdown(wait=False)

    def test_admin_image_task_does_not_create_quota_row(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            quota = QuotaService(AuthQuotaStore(Path(tmp_dir) / "auth_quota.db"))
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", quota_service=quota)

            service.submit_generation(
                OWNER,
                client_task_id="admin-no-quota",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            wait_for_task(service, OWNER, "admin-no-quota", "success")
            self.assertIsNone(quota.get_quota(OWNER["id"]))

    def test_duplicate_submit_uses_existing_task(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            calls = 0

            def handler(_payload):
                nonlocal calls
                calls += 1
                time.sleep(0.05)
                return {"data": [{"url": "http://example.test/image.png"}]}

            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler)
            first = service.submit_generation(
                OWNER,
                client_task_id="task-1",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            second = service.submit_generation(
                OWNER,
                client_task_id="task-1",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            self.assertEqual(first["id"], "task-1")
            self.assertEqual(second["id"], "task-1")
            task = wait_for_task(service, OWNER, "task-1", "success")
            self.assertEqual(task["data"][0]["url"], "http://example.test/image.png")
            self.assertEqual(calls, 1)

    def test_edit_inputs_are_spooled_until_worker_execution_then_removed(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            observed = {}

            def handler(payload):
                observed["images"] = payload["images"]
                observed["masks"] = payload["mask"]
                observed["spool_files_during_run"] = list(
                    (root / "image-task-spool").glob("image-task-*.bin")
                )
                return {"data": [{"url": "http://example.test/edited.png"}]}

            service = self.make_service(root / "image_tasks.json", handler)
            service.submit_edit(
                OWNER,
                client_task_id="spooled-edit",
                prompt="remove the background",
                model="gpt-image-2",
                size=None,
                images=[(b"source-image", "source.png", "image/png")],
                masks=[(b"source-mask", "mask.png", "image/png")],
            )

            task = wait_for_task(service, OWNER, "spooled-edit", "success")
            self.assertEqual(task["data"][0]["url"], "http://example.test/edited.png")
            self.assertEqual(observed["images"], [(b"source-image", "source.png", "image/png")])
            self.assertEqual(observed["masks"], [(b"source-mask", "mask.png", "image/png")])
            self.assertEqual(len(observed["spool_files_during_run"]), 2)
            self.assertEqual(list((root / "image-task-spool").glob("image-task-*.bin")), [])
            service.shutdown(wait=True)

    def test_edit_spool_is_removed_when_handler_fails(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)

            def handler(_payload):
                raise RuntimeError("edit failed")

            service = self.make_service(root / "image_tasks.json", handler)
            service.submit_edit(
                OWNER,
                client_task_id="spooled-failure",
                prompt="edit",
                model="gpt-image-2",
                size=None,
                images=[(b"source-image", "source.png", "image/png")],
            )

            task = wait_for_task(service, OWNER, "spooled-failure", "error")
            self.assertIn("edit failed", task["error"])
            self.assertEqual(list((root / "image-task-spool").glob("image-task-*.bin")), [])
            service.shutdown(wait=True)

    def test_invalid_edit_task_id_does_not_leave_spooled_inputs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            service = self.make_service(root / "image_tasks.json")

            with self.assertRaises(ValueError):
                service.submit_edit(
                    OWNER,
                    client_task_id="",
                    prompt="edit",
                    model="gpt-image-2",
                    size=None,
                    images=[(b"source-image", "source.png", "image/png")],
                )

            self.assertEqual(list((root / "image-task-spool").glob("image-task-*.bin")), [])
            service.shutdown(wait=True)

    def test_different_owner_cannot_query_task(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            service.submit_generation(
                OWNER,
                client_task_id="private-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            wait_for_task(service, OWNER, "private-task", "success")
            result = service.list_tasks(OTHER_OWNER, ["private-task"])

            self.assertEqual(result["items"], [])
            self.assertEqual(result["missing_ids"], ["private-task"])

    def test_success_task_persists_to_new_service_instance(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path)
            service.submit_generation(
                OWNER,
                client_task_id="persisted-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "persisted-task", "success")

            reloaded = self.make_service(path)
            result = reloaded.list_tasks(OWNER, ["persisted-task"])

            self.assertEqual(result["missing_ids"], [])
            self.assertEqual(result["items"][0]["status"], "success")
            self.assertNotIn("data", result["items"][0])

    def test_success_task_keeps_result_in_memory_without_persisting_image_data(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            encoded = base64.b64encode(b"generated-image").decode("ascii")

            def handler(_payload):
                return {"data": [{"b64_json": encoded, "revised_prompt": "cat"}]}

            service = self.make_service(path, handler)
            service.submit_generation(
                OWNER,
                client_task_id="memory-only-result",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            current = wait_for_task(service, OWNER, "memory-only-result", "success")
            self.assertEqual(current["data"][0]["b64_json"], encoded)

            persisted = json.loads(path.read_text(encoding="utf-8"))
            persisted_task = persisted["tasks"][0]
            self.assertNotIn("data", persisted_task)

            reloaded = self.make_service(path, handler)
            reloaded_task = reloaded.list_tasks(OWNER, ["memory-only-result"])["items"][0]
            self.assertEqual(reloaded_task["status"], "success")
            self.assertNotIn("data", reloaded_task)

    def test_legacy_task_data_survives_metadata_save(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            legacy_data = [{"url": "http://local.test/images/legacy.png"}]
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            path.write_text(
                json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "legacy-task",
                                "owner_id": OWNER["id"],
                                "status": "success",
                                "mode": "generate",
                                "model": "gpt-image-2",
                                "created_at": now,
                                "updated_at": now,
                                "data": legacy_data,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            service = self.make_service(path)
            service._update_task("owner-1:legacy-task", progress="metadata-only-update")
            service._update_task(
                "owner-1:legacy-task",
                data=[{"url": "data:image/png;base64,new-result"}],
            )

            persisted = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["tasks"][0]["data"], legacy_data)

    def test_collect_image_urls_skips_inline_data_urls(self):
        self.assertEqual(
            _collect_image_urls(
                [
                    {"url": "data:image/png;base64,c2VjcmV0"},
                    {"url": "https://files.example.test/image.png"},
                ]
            ),
            ["https://files.example.test/image.png"],
        )

    def test_resume_poll_uses_lease_token_and_keeps_remote_urls(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path)
            key = "owner-1:resume-task"
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            service._tasks[key] = {
                "id": "resume-task",
                "owner_id": OWNER["id"],
                "status": "running",
                "mode": "generate",
                "model": "gpt-image-2",
                "quality": "auto",
                "size": "",
                "created_at": now,
                "updated_at": now,
            }
            backend = mock.Mock()
            backend._poll_image_results.return_value = (["file-1"], [])
            backend.resolve_conversation_image_urls.return_value = ["https://files.example.test/image.png"]
            backend.download_image_bytes.return_value = [b"image-bytes"]

            with mock.patch(
                "services.openai_backend_api.OpenAIBackendAPI",
                return_value=backend,
            ) as backend_factory:
                service._run_resume_poll(
                    key,
                    "conversation-1",
                    1.0,
                    OWNER,
                    "generate",
                    "gpt-image-2",
                    access_token="account-token",
                )

            self.assertEqual(service._tasks[key].get("status"), "success", service._tasks[key])
            backend_factory.assert_called_once_with(access_token="account-token")
            self.assertEqual(
                service._tasks[key]["data"],
                [{"url": "https://files.example.test/image.png"}],
            )
            backend.download_image_bytes.assert_not_called()

    def test_startup_marks_unfinished_tasks_as_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            path.write_text(
                json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "queued-task",
                                "owner_id": "owner-1",
                                "status": "queued",
                                "mode": "generate",
                                "model": "gpt-image-2",
                                "created_at": "2099-01-01 00:00:00",
                                "updated_at": "2099-01-01 00:00:00",
                            },
                            {
                                "id": "running-task",
                                "owner_id": "owner-1",
                                "status": "running",
                                "mode": "generate",
                                "model": "gpt-image-2",
                                "created_at": "2099-01-01 00:00:00",
                                "updated_at": "2099-01-01 00:00:00",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            service = self.make_service(path)
            result = service.list_tasks(OWNER, ["queued-task", "running-task"])

            self.assertEqual([item["status"] for item in result["items"]], ["error", "error"])
            self.assertTrue(all("已中断" in item.get("error", "") for item in result["items"]))


if __name__ == "__main__":
    unittest.main()
