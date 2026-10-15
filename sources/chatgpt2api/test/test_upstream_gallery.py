import base64, json, os
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
import pytest
from PIL import Image
from services.protocol import openai_v1_image_generations as generate, openai_v1_image_edit as edit
from services.image_storage_service import ImageStorageService
from services.image_task_service import _task_result_metadata
from services.config import config

@pytest.mark.parametrize("mode", ["generate", "edit"])
@pytest.mark.parametrize("response_format", ["url", "b64_json"])
def test_upstream_result_is_saved_and_listed(tmp_path, mode, response_format):
    buf=BytesIO()
    Image.new("RGB",(2,2),"blue").save(buf,format="PNG")
    raw=buf.getvalue()
    encoded=base64.b64encode(raw).decode()
    private=tmp_path/"private.json"
    private.write_text(json.dumps({"base_url":"http://upstream.invalid","api_key":"test-only"}))
    storage=ImageStorageService(tmp_path/"index.json")
    root=tmp_path/"images"
    with patch.dict(os.environ,{"IMAGE_UPSTREAM_CONFIG":str(private)}), patch("urllib.request.urlopen",return_value=BytesIO(json.dumps({"data":[{"b64_json":encoded}]}).encode())), patch("services.image_storage_service.image_storage_service",storage), patch.object(type(config),"images_dir",new_callable=lambda:property(lambda self:root)), patch.object(config,"cleanup_old_images"), patch.object(storage,"settings",return_value={"mode":"local"}):
        body={"prompt":"test","model":"gpt-image-2.5-sunburst","response_format":response_format,"base_url":"https://example.test"}
        if mode=="edit":
            body["images"]=[(raw,"input.png","image/png")]
        result=(edit if mode=="edit" else generate).handle(body)
        gallery=storage.list_items("https://example.test")
        assert len(gallery)==1, "Successful upstream result missing from image manager"
        assert storage.get_bytes(gallery[0]["rel"])==raw
        if response_format=="url":
            assert result["data"][0]["url"].startswith("https://example.test/images/")
            assert _task_result_metadata({"data":result["data"]})["available"]
        else:
            assert result["data"][0]["b64_json"]==encoded
