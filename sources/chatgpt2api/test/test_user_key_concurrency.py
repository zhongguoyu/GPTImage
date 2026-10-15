import unittest

from services.user_key_concurrency import UserKeyConcurrencyLimiter


USER = {"id": "user-1", "role": "user"}
OTHER_USER = {"id": "user-2", "role": "user"}
ADMIN = {"id": "admin", "role": "admin"}


class UserKeyConcurrencyLimiterTests(unittest.TestCase):
    def setUp(self):
        self.limits = {"text": 1, "image": 1}
        self.limiter = UserKeyConcurrencyLimiter(
            text_limit_getter=lambda: self.limits["text"],
            image_limit_getter=lambda: self.limits["image"],
        )

    def test_text_and_image_limits_are_independent_per_user(self):
        text = self.limiter.try_acquire(USER, "text")
        image = self.limiter.try_acquire(USER, "image")
        self.assertIsNotNone(text)
        self.assertIsNotNone(image)
        self.assertIsNone(self.limiter.try_acquire(USER, "text"))
        self.assertIsNone(self.limiter.try_acquire(USER, "image"))
        self.assertIsNotNone(self.limiter.try_acquire(OTHER_USER, "text"))

    def test_admin_is_not_limited(self):
        leases = [self.limiter.try_acquire(ADMIN, "text") for _ in range(20)]
        self.assertTrue(all(lease is not None for lease in leases))

    def test_release_is_idempotent(self):
        lease = self.limiter.try_acquire(USER, "text")
        self.assertIsNotNone(lease)
        lease.release()
        lease.release()
        self.assertIsNotNone(self.limiter.try_acquire(USER, "text"))
        self.assertEqual(self.limiter.active_count(USER, "text"), 1)

    def test_limit_changes_apply_to_new_admissions(self):
        first = self.limiter.try_acquire(USER, "text")
        self.limits["text"] = 2
        second = self.limiter.try_acquire(USER, "text")
        self.limits["text"] = 1
        third = self.limiter.try_acquire(USER, "text")
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertIsNone(third)

    def test_each_user_key_uses_its_own_limit_and_updates_immediately(self):
        limits = {
            "user-1": {"text": 1, "image": 2},
            "user-2": {"text": 2, "image": 1},
        }
        limiter = UserKeyConcurrencyLimiter(
            limit_getter=lambda identity, request_type: limits[identity["id"]][request_type],
        )

        first = limiter.try_acquire(USER, "text")
        self.assertIsNotNone(first)
        self.assertIsNone(limiter.try_acquire(USER, "text"))
        self.assertIsNotNone(limiter.try_acquire(OTHER_USER, "text"))
        self.assertIsNotNone(limiter.try_acquire(OTHER_USER, "text"))
        self.assertIsNone(limiter.try_acquire(OTHER_USER, "text"))

        limits["user-1"]["text"] = 2
        self.assertIsNotNone(limiter.try_acquire(USER, "text"))

    def test_recovered_lease_is_counted_even_when_limit_is_full(self):
        first = self.limiter.try_acquire(USER, "image")
        recovered = self.limiter.acquire_existing(USER, "image")
        self.assertIsNotNone(first)
        self.assertIsNotNone(recovered)
        self.assertEqual(self.limiter.active_count(USER, "image"), 2)
        first.release()
        recovered.release()
        self.assertEqual(self.limiter.active_count(USER, "image"), 0)


if __name__ == "__main__":
    unittest.main()
