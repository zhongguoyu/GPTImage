import json,os,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from io import BytesIO
from services.protocol import openai_v1_image_generations as api
class UpstreamTests(unittest.TestCase):
 def test_web_generation_routes_to_sub2_and_preserves_url_format(self):
  with tempfile.TemporaryDirectory() as tmp:
   f=Path(tmp)/"private.json";f.write_text(json.dumps({"base_url":"http://sub2api:8080","api_key":"test-only"}))
   with patch.dict(os.environ,{"IMAGE_UPSTREAM_CONFIG":str(f)}), patch("urllib.request.urlopen",return_value=BytesIO(b'{"data":[{"b64_json":"aW1hZ2U="}]}')) as call, patch.object(api,"stream_image_outputs_with_pool",side_effect=AssertionError("web backend must not run")):
    out=api.handle({"model":"gpt-image-2","prompt":"cup","response_format":"url","quality":"auto"})
    self.assertIn("/images/", out["data"][0]["url"])
    req=call.call_args.args[0]
    self.assertEqual(req.full_url,"http://sub2api:8080/v1/images/generations")
    self.assertEqual(json.loads(req.data)["quality"],"auto")
 def test_missing_image_is_not_reported_as_success(self):
  with tempfile.TemporaryDirectory() as tmp:
   f=Path(tmp)/"private.json";f.write_text(json.dumps({"base_url":"http://sub2api:8080","api_key":"test-only"}))
   with patch.dict(os.environ,{"IMAGE_UPSTREAM_CONFIG":str(f)}),patch("urllib.request.urlopen",return_value=BytesIO(b'{"data":[]}')):
    with self.assertRaisesRegex(Exception,"no image data"):
     api.handle({"prompt":"cup"})
