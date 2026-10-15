from __future__ import annotations

import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from services.quota_service import (
    AuthQuotaStore,
    QuotaExceededError,
    QuotaLimitError,
    QuotaService,
)


def user_identity(key_id: str = "user-1") -> dict[str, object]:
    return {"id": key_id, "name": "普通用户", "role": "user"}


def admin_identity() -> dict[str, object]:
    return {"id": "admin", "name": "管理员", "role": "admin"}


def make_service(root: Path) -> QuotaService:
    service = QuotaService(AuthQuotaStore(root / "auth_quota.db"))
    service.initialize()
    return service


def reserve_text(service: QuotaService, index: int):
    try:
        return service.reserve(
            user_identity(),
            "text",
            1,
            request_id=f"request-{index}",
            endpoint="/v1/chat/completions",
        )
    except QuotaExceededError:
        return None


class QuotaServiceTests(unittest.TestCase):
    def test_worker_connections_are_reclaimed_after_thread_exit(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = AuthQuotaStore(Path(tmp_dir) / "auth_quota.db")
            store.initialize()
            baseline = store.connection_count()

            def read() -> None:
                store._connection().execute("SELECT 1").fetchone()

            thread = threading.Thread(target=read)
            thread.start()
            thread.join()

            self.assertEqual(store.connection_count(), baseline + 1)
            store.reap_dead_threads()
            self.assertEqual(store.connection_count(), baseline)
            store.close()

    def test_new_user_has_zero_quota_and_admin_bypasses(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            service.ensure_key("user-1")

            quota = service.get_quota("user-1")
            self.assertEqual(quota["text_remaining"], 0)
            self.assertFalse(quota["text_unlimited"])
            self.assertFalse(quota["image_unlimited"])
            self.assertEqual(quota["text_concurrency"], 4)
            self.assertEqual(quota["image_concurrency"], 1)
            with self.assertRaises(QuotaExceededError):
                service.reserve(
                    user_identity(),
                    "text",
                    1,
                    request_id="r-1",
                    endpoint="/v1/chat/completions",
                )

            reservation = service.reserve(
                admin_identity(),
                "image",
                4,
                request_id="admin-1",
                endpoint="/v1/images/generations",
            )
            self.assertTrue(reservation.admin_bypass)
            self.assertIsNone(service.get_quota("admin"))

    def test_concurrency_limits_are_per_key_and_update_immediately(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            service.ensure_key("user-1")
            service.ensure_key("user-2")

            updated = service.set_limits(
                "user-1",
                text_concurrency=2,
                image_concurrency=3,
            )

            self.assertEqual(updated["text_concurrency"], 2)
            self.assertEqual(updated["image_concurrency"], 3)
            self.assertEqual(service.get_concurrency_limit("user-1", "text"), 2)
            self.assertEqual(service.get_concurrency_limit("user-1", "image"), 3)
            self.assertEqual(service.get_concurrency_limit("user-2", "text"), 4)
            self.assertEqual(service.get_concurrency_limit("user-2", "image"), 1)

    def test_concurrency_limits_must_be_positive_integers(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            with self.assertRaisesRegex(ValueError, "concurrency limit"):
                service.set_limits("user-1", text_concurrency=0)
            with self.assertRaisesRegex(ValueError, "concurrency limit"):
                service.set_limits("user-1", image_concurrency=True)

    def test_old_quota_table_is_migrated_with_concurrency_defaults(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "auth_quota.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE key_quotas (
                    key_id TEXT PRIMARY KEY,
                    text_limit INTEGER NOT NULL DEFAULT 0,
                    text_used INTEGER NOT NULL DEFAULT 0,
                    text_reserved INTEGER NOT NULL DEFAULT 0,
                    image_limit INTEGER NOT NULL DEFAULT 0,
                    image_used INTEGER NOT NULL DEFAULT 0,
                    image_reserved INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    deleted_at REAL
                )
                """
            )
            connection.execute(
                "INSERT INTO key_quotas (key_id, created_at, updated_at) VALUES (?, ?, ?)",
                ("user-1", 1, 1),
            )
            connection.commit()
            connection.close()

            service = make_service(Path(tmp_dir))
            quota = service.get_quota("user-1")
            self.assertFalse(quota["text_unlimited"])
            self.assertFalse(quota["image_unlimited"])
            self.assertEqual(quota["text_concurrency"], 4)
            self.assertEqual(quota["image_concurrency"], 1)

    def test_reserve_commit_and_release_keep_remaining_consistent(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            service.set_limits("user-1", text_limit=2, image_limit=4)

            text = service.reserve(
                user_identity(),
                "text",
                1,
                request_id="t-1",
                endpoint="/v1/chat/completions",
            )
            image = service.reserve(
                user_identity(),
                "image",
                4,
                request_id="i-1",
                endpoint="/v1/images/generations",
            )
            quota = service.get_quota("user-1")
            self.assertEqual(
                (quota["text_used"], quota["text_reserved"], quota["text_remaining"]),
                (0, 1, 1),
            )
            self.assertEqual(
                (quota["image_used"], quota["image_reserved"], quota["image_remaining"]),
                (0, 4, 0),
            )

            self.assertTrue(service.commit(text.reservation_id, 1))
            self.assertTrue(service.commit(image.reservation_id, 3))
            self.assertFalse(service.release(image.reservation_id))
            self.assertFalse(service.commit(image.reservation_id, 1))
            quota = service.get_quota("user-1")
            self.assertEqual(
                (quota["text_used"], quota["text_reserved"], quota["text_remaining"]),
                (1, 0, 1),
            )
            self.assertEqual(
                (quota["image_used"], quota["image_reserved"], quota["image_remaining"]),
                (3, 0, 1),
            )

    def test_limit_update_cannot_drop_below_used_or_reserved(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            service.set_limits("user-1", text_limit=3, image_limit=3)
            reservation = service.reserve(
                user_identity(),
                "text",
                2,
                request_id="r-1",
                endpoint="/v1/chat/completions",
            )
            service.commit(reservation.reservation_id, 1)
            with self.assertRaises(QuotaLimitError):
                service.set_limits("user-1", text_limit=0, image_limit=3)

            reserved = service.reserve(
                user_identity(),
                "text",
                1,
                request_id="r-2",
                endpoint="/v1/chat/completions",
            )
            self.assertIsNotNone(reserved)
            with self.assertRaises(QuotaLimitError):
                service.set_limits("user-1", text_limit=1, image_limit=3)

    def test_expired_reservation_is_released_once(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            service.set_limits("user-1", text_limit=1, image_limit=1)
            reservation = service.reserve(
                user_identity(),
                "text",
                1,
                request_id="r-1",
                endpoint="/v1/chat/completions",
                expires_at=time.time() - 1,
            )
            self.assertEqual(service.recover_expired(now=time.time()), 1)
            self.assertEqual(service.recover_expired(now=time.time()), 0)
            self.assertEqual(service.get_quota("user-1")["text_reserved"], 0)
            self.assertEqual(service.get_reservation(reservation.reservation_id)["status"], "expired")

    def test_active_reservation_can_be_renewed_but_expired_one_cannot(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            service.set_limits("user-1", text_limit=2, image_limit=0)
            active = service.reserve(
                user_identity(),
                "text",
                1,
                request_id="active",
                endpoint="/v1/chat/completions",
                expires_at=time.time() + 30,
            )
            old_expiry = service.get_reservation(active.reservation_id)["expires_at"]
            self.assertTrue(service.renew(active.reservation_id, expires_at=time.time() + 120))
            self.assertGreater(service.get_reservation(active.reservation_id)["expires_at"], old_expiry)

            expired = service.reserve(
                user_identity(),
                "text",
                1,
                request_id="expired",
                endpoint="/v1/chat/completions",
                expires_at=time.time() - 1,
            )
            self.assertFalse(service.renew(expired.reservation_id, expires_at=time.time() + 120))

    def test_concurrent_reservations_never_exceed_limit(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = make_service(Path(tmp_dir))
            service.set_limits("user-1", text_limit=8, image_limit=0)
            with ThreadPoolExecutor(max_workers=16) as pool:
                results = list(pool.map(lambda index: reserve_text(service, index), range(16)))
            self.assertEqual(sum(result is not None for result in results), 8)
            self.assertLessEqual(service.get_quota("user-1")["text_reserved"], 8)


if __name__ == "__main__":
    unittest.main()
