import unittest,json,os,tempfile,base64
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from services.protocol import openai_v1_image_edit as api
from services.account_service import AccountService
class CompleteRouteTests(unittest.TestCase):
 def test_edit_uses_multipart_and_keeps_comments(self):
  from PIL import Image
  b=BytesIO();Image.new("RGB",(2,2),"blue").save(b,format="PNG")
  with tempfile.TemporaryDirectory() as tmp:
   f=Path(tmp)/"cfg";f.write_text(json.dumps({"base_url":"http://sub2api:8080","api_key":"test"}))
   with patch.dict(os.environ,{"IMAGE_UPSTREAM_CONFIG":str(f)}),patch("urllib.request.urlopen",return_value=BytesIO(b'{"data":[{"b64_json":"aW1hZ2U="}]}')) as call:
    out=api.handle({"prompt":"make red","images":[(b.getvalue(),"a.png","image/png")],"comments":[{"x":0.5,"y":0.5,"text":"red here"}]})
    req=call.call_args.args[0]
    self.assertTrue(req.full_url.endswith("/v1/images/edits"))
    self.assertIn(b'name="image[]"',req.data)
    self.assertIn(b"red here",req.data)
    self.assertEqual(len(out["data"]),1)
 def test_refresh_uses_sub2_metadata_without_web_probe(self):
  updates={"status":"正常","account_info_source":"sub2api","image_quota_known":False}
  obj=SimpleNamespace(_mark_account_info_pending=lambda token:None,get_account=lambda token:{"source_type":"codex"},_record_refresh_success=lambda token:None,
      update_account=lambda token,data,quiet=False:data)
  with patch("services.sub2api_account_info.fetch",return_value=updates):
   self.assertEqual(AccountService.fetch_remote_info(obj,"test"),updates)
 def test_refresh_failure_is_not_silently_successful(self):
  obj=SimpleNamespace(_mark_account_info_pending=lambda token:None,get_account=lambda token:{"source_type":"codex"})
  with patch("services.sub2api_account_info.fetch",side_effect=RuntimeError("Sub2API unavailable")):
   with self.assertRaisesRegex(RuntimeError,"unavailable"):AccountService.fetch_remote_info(obj,"test")

 def test_compact_progress_preserves_source_without_credentials(self):
  result=AccountService._compact_progress_result({"items":[{"account_info_source":"sub2api","access_token":"secret"}],"refreshed":1})
  self.assertEqual(result["account_info_source"],"sub2api")
  self.assertNotIn("secret",str(result))
