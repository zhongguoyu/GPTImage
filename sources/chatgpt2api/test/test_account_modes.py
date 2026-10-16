import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi import HTTPException
from services import routing_mode as route
from services.account_metadata import native_metadata, display_metadata
from services.account_service import account_service

class AccountModeTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        for p in (patch.object(route, "mode_path", return_value=Path(tmp.name)/"mode.json"),
                  patch.object(route, "_busy_tasks", return_value=False)):
            p.start(); self.addCleanup(p.stop)
        route.switch("native")

    def test_native_zero_is_known_and_clears_gateway_fields(self):
        a = native_metadata({"quota":0,"limits_progress":[{"feature_name":"image_gen","remaining":0}]})
        self.assertTrue(a["image_quota_known"])
        self.assertFalse(display_metadata(a)["account_info_stale"])
        self.assertEqual(a["sub2api_usage"], {})
        self.assertIsNone(a["sub2api_account_id"])

    def test_missing_image_quota_is_not_a_reported_zero(self):
        for limits in ([], [{"feature_name":"image_gen"}], [{"feature_name":"text","remaining":100}]):
            self.assertFalse(native_metadata({"limits_progress":limits})["image_quota_known"])

    def test_switch_back_invalidates_earlier_same_mode_snapshot(self):
        a = native_metadata({"quota":7,"limits_progress":[{"feature_name":"image_gen","remaining":7}]})
        route.switch("native")
        result = display_metadata(a)
        self.assertTrue(result["account_info_stale"])
        self.assertFalse(result["image_quota_known"])
        self.assertEqual(result["quota"],0)
        self.assertEqual(a["quota"],7)

    def test_gateway_never_inherits_native_quota(self):
        with patch("services.sub2_gateway.settings",return_value={}): route.switch("sub2api")
        a=native_metadata({"quota":9,"restore_at":"later","limits_progress":[]})
        result=display_metadata(a)
        self.assertTrue(result["account_info_stale"])
        self.assertIsNone(result["restore_at"])
        self.assertEqual(result["quota"],0)

    def test_refresh_guard_blocks_switch_and_releases_on_error(self):
        @route.guard_account_activity
        def run():
            with self.assertRaises(HTTPException): route.switch("native")
            raise RuntimeError("test")
        with self.assertRaises(RuntimeError): run()
        self.assertEqual(route._active_requests,0)

    def test_switch_refreshes_and_reports_failure_without_rollback(self):
        def refresh(*args,**kwargs):
            with self.assertRaises(HTTPException): route.switch("native")
            return {"refreshed":1,"errors":[{}]}
        with patch.object(account_service,"list_tokens",return_value=["test"]), patch.object(account_service,"refresh_accounts",side_effect=refresh):
            result=route.switch_and_refresh("native")
        self.assertEqual(result["account_refresh"],{"refreshed":1,"failed":1})
        with patch.object(account_service,"refresh_accounts",side_effect=RuntimeError("private")):
            result=route.switch_and_refresh("native")
        self.assertNotIn("private",str(result))
        self.assertEqual(route._active_requests,0)

    def test_gateway_refresh_clears_web_metadata(self):
        from services.sub2api_account_info import fetch
        with patch("services.sub2api_account_info._lookup",return_value=({}, {"id":1,"status":"active","schedulable":True})):
            result=fetch({"quota":8,"restore_at":"old"})
        self.assertEqual(result["quota"],0)
        self.assertIsNone(result["restore_at"])
        self.assertFalse(result["image_quota_known"])
