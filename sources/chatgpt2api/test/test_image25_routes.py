import unittest,os,json,tempfile
from unittest.mock import patch
from pathlib import Path
from io import BytesIO
from utils.helper import is_supported_image_model,split_image_model
from services.protocol import openai_v1_models,openai_v1_image_generations
from services.scheduler.service import SchedulerService
from services.scheduler.models import Lane
MODELS=("gpt-image-2.5-flare","gpt-image-2.5-sunburst")
class Image25Tests(unittest.TestCase):
 def test_models_are_recognized_without_aliasing(self):
  for model in MODELS:
   self.assertTrue(is_supported_image_model(model))
   self.assertEqual(split_image_model(model),(None,model))
 def test_catalog_exposes_models_when_images_upstream_enabled(self):
  with patch.dict(os.environ,{"IMAGE_UPSTREAM_CONFIG":"/configured"}),patch.object(openai_v1_models.model_catalog_service,"list_models",return_value={"data":[]}),patch.object(openai_v1_models.account_service,"list_accounts",return_value=[{"source_type":"codex","type":"ProLite"}]):
   ids={x["id"] for x in openai_v1_models.list_models()["data"]}
   self.assertTrue(set(MODELS)<=ids)
 def test_generation_forwards_exact_model(self):
  with tempfile.TemporaryDirectory() as tmp:
   f=Path(tmp)/"config";f.write_text(json.dumps({"base_url":"http://sub2api:8080","api_key":"test"}))
   for model in MODELS:
    with patch.dict(os.environ,{"IMAGE_UPSTREAM_CONFIG":str(f)}),patch("urllib.request.urlopen",return_value=BytesIO(b'{"data":[{"b64_json":"aW1hZ2U="}]}')) as call:
     openai_v1_image_generations.handle({"model":model,"prompt":"cup"})
     self.assertEqual(json.loads(call.call_args.args[0].data)["model"],model)
 def test_scheduler_preserves_model(self):
  for model in MODELS:
   req=SchedulerService._classify_image(None,{"model":model},Lane.IMAGE_GENERATION)
   self.assertEqual(req.model,model)
