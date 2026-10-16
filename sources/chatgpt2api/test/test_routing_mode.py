import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi import HTTPException
from services import routing_mode as route, sub2_gateway
from services.protocol import image_upstream
from services.sub2api_account_info import fetch

class RoutingModeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        p = patch.object(route, "mode_path", return_value=Path(self.tmp.name)/"route.json")
        p.start(); self.addCleanup(p.stop)
        p = patch.dict(os.environ, {"SUB2API_ONLY":"1", "IMAGE_UPSTREAM_CONFIG":"/configured"})
        p.start(); self.addCleanup(p.stop)
        p = patch.object(route, "_busy_tasks", return_value=False)
        self.busy = p.start(); self.addCleanup(p.stop)
    def test_native_overrides_both_environment_routes_and_account_sync(self):
        route.switch("native")
        self.assertFalse(sub2_gateway.enabled())
        self.assertFalse(image_upstream.configured())
        self.assertIsNone(fetch({"source_type":"codex"}))
        self.assertEqual(json.loads(route.mode_path().read_text())["mode"], "native")
        from api.ai import _scheduler_enabled
        from services.config import config
        with patch.object(config, "get_scheduler_settings_dict", return_value={"enabled":True}):
            self.assertTrue(_scheduler_enabled())
    def test_sub2_can_be_restored_without_deleting_credentials(self):
        route.switch("native")
        with patch.object(sub2_gateway, "settings", return_value={}):
            route.switch("sub2api")
        self.assertTrue(sub2_gateway.enabled()); self.assertTrue(image_upstream.configured())
        from api.ai import _image_scheduler_runner
        self.assertIsNone(_image_scheduler_runner("gpt-image-2", {}, None))
    def test_busy_or_invalid_mode_leaves_original_choice(self):
        route.switch("native")
        self.busy.return_value=True
        with self.assertRaises(HTTPException) as error: route.switch("sub2api")
        self.assertEqual(error.exception.status_code,409)
        self.assertEqual(route.state()["mode"],"native")
        with self.assertRaises(HTTPException): route.switch("other")
    def test_missing_sub2_configuration_does_not_change_mode(self):
        route.switch("native")
        with self.assertRaises(HTTPException): route.switch("sub2api")
        self.assertEqual(route.state()["mode"],"native")
    def test_request_guard_covers_entire_stream_and_releases_after_error(self):
        async def app(scope, receive, send):
            with self.assertRaises(HTTPException) as error: route.switch("native")
            self.assertEqual(error.exception.status_code,409)
            raise RuntimeError("ended")
        with self.assertRaises(RuntimeError):
            asyncio.run(route.RoutingActivityMiddleware(app)({"type":"http","path":"/v1/responses"},None,None))
        self.assertEqual(route._active_requests,0)
        route.switch("native")
    def test_unset_preserves_existing_environment_behavior(self):
        self.assertTrue(sub2_gateway.enabled()); self.assertTrue(image_upstream.configured())
        with patch.dict(os.environ,{"SUB2API_ONLY":"0"}):
            self.assertFalse(sub2_gateway.enabled()); self.assertTrue(image_upstream.configured())

    def test_native_images_archive_without_using_sub2_transport(self):
        route.switch("native")
        from services.protocol.conversation import format_image_result
        from services.image_storage_service import image_storage_service
        from types import SimpleNamespace
        with patch.object(image_storage_service,"save",return_value=SimpleNamespace(url="https://images.example.com/images/test")) as save:
            result=format_image_result([{"b64_json":"aW1hZ2U="}],"test","url")
        self.assertEqual(result["data"][0]["url"],"https://images.example.com/images/test")
        self.assertEqual(save.call_args.args[0],b"image")
    def test_native_generation_and_edit_use_original_handlers(self):
        route.switch("native")
        from services.protocol import openai_v1_image_generations as gen, openai_v1_image_edit as edit
        with patch.object(gen,"stream_image_outputs_with_pool",return_value=[]) as native, patch.object(gen,"format_outputs",return_value={}), patch.object(image_upstream,"generate",side_effect=AssertionError("Sub2 called")):
            gen.handle({"prompt":"test"})
        native.assert_called_once()
        with patch.object(edit,"build_request",return_value=object()), patch.object(edit,"stream_image_outputs_with_pool",return_value=[]) as native, patch.object(edit,"format_outputs",return_value={}), patch.object(image_upstream,"edit",side_effect=AssertionError("Sub2 called")):
            edit.handle({"prompt":"test"})
        native.assert_called_once()
    def test_switch_api_requires_admin(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from api.system import create_router
        from services.config import config
        app=FastAPI(); app.include_router(create_router("test"))
        with TestClient(app) as client:
            self.assertEqual(client.post("/api/routing-mode",json={"mode":"native"}).status_code,401)
            result=client.post("/api/routing-mode",json={"mode":"native"},headers={"Authorization":"Bearer "+config.auth_key})
            self.assertEqual(result.status_code,200)
            self.assertEqual(result.json()["mode"],"native")

    def test_native_pool_status_uses_local_accounts(self):
        route.switch("native")
        from services.image_operational_status import get_pool
        from services.account_service import account_service
        with patch.object(account_service,"list_accounts",return_value=[{"status":"正常"}]) as accounts:
            pool=get_pool()
        accounts.assert_called_once()
        self.assertTrue(pool[0]["schedulable"])
        self.assertEqual(pool[0]["extra"],{})
