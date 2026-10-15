"""Optional standard Images API upstream; credentials stay in a private file."""
from __future__ import annotations
import json
import os
import time
import urllib.request
import urllib.error
from pathlib import Path
from services.protocol.conversation import ImageGenerationError, ImageOutput, format_image_result

def configured():
    return bool(os.environ.get("IMAGE_UPSTREAM_CONFIG"))

def _archive_items(items, body):
    """Archive upstream bytes through the gallery's existing storage/index service."""
    import base64
    from services.image_storage_service import image_storage_service
    archived = []
    for item in items:
        try:
            raw = base64.b64decode(item["b64_json"], validate=True)
        except (ValueError, TypeError):
            raise ImageGenerationError("Sub2API returned invalid image data", status_code=502) from None
        stored = image_storage_service.save(raw, str(body.get("base_url") or "") or None)
        archived.append({**item, "url": stored.url})
    return archived

def generate(body):
    path = os.environ["IMAGE_UPSTREAM_CONFIG"]
    cfg = json.loads(Path(path).read_text())
    payload = {k: body[k] for k in ("model", "prompt", "n", "size", "quality") if k in body and body[k] is not None}
    payload["response_format"] = "b64_json"
    callback = body.get("progress_callback")
    if callback:
        callback("generating", "")
    req = urllib.request.Request(
        cfg["base_url"].rstrip("/") + "/v1/images/generations",
        data=json.dumps(payload).encode(),
        headers={"Authorization": "Bearer " + cfg["api_key"], "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=240) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        # Never include response headers or credentials in a user-visible error.
        raise ImageGenerationError(
            "Sub2API Images API returned HTTP " + str(exc.code),
            status_code=502,
        ) from None
    items = result.get("data") or []
    if not items or not all(isinstance(x, dict) and x.get("b64_json") for x in items):
        raise ImageGenerationError("Sub2API returned no image data", status_code=502)
    data = format_image_result(
        _archive_items(items, body), str(body.get("prompt") or ""),
        str(body.get("response_format") or "b64_json"), int(time.time()),
    )["data"]
    yield ImageOutput(kind="result", model=str(body.get("model") or "gpt-image-2"),
                      index=1, total=int(body.get("n") or 1), data=data)

def edit(body, request):
    import base64
    import uuid
    cfg = json.loads(Path(os.environ["IMAGE_UPSTREAM_CONFIG"]).read_text())
    boundary = "----chatgpt2api-" + uuid.uuid4().hex
    parts = []
    def field(name, value, filename=None, content_type=None):
        header = 'Content-Disposition: form-data; name="' + name + '"'
        if filename:
            header += '; filename="' + filename + '"'
        chunk = "--" + boundary + "\r\n" + header + "\r\n"
        if content_type:
            chunk += "Content-Type: " + content_type + "\r\n"
        parts.append(chunk.encode() + b"\r\n" + (value if isinstance(value, bytes) else str(value).encode()) + b"\r\n")
    prompt = request.prompt
    for comment in request.comments or []:
        prompt += f"\nImage {comment.image_index + 1}, point ({comment.x * 100:.1f}%, {comment.y * 100:.1f}%): {comment.text}"
    for key,value in {"model":request.model,"prompt":prompt,"n":request.n,"size":request.size,
                      "quality":request.quality,"response_format":"b64_json"}.items():
        if value is not None: field(key,value)
    for i,encoded in enumerate(request.images or []):
        raw = base64.b64decode(encoded.split(",",1)[1] if encoded.startswith("data:") else encoded)
        field("image[]", raw, f"image-{i}.png", "image/png")
    data = b"".join(parts) + ("--" + boundary + "--\r\n").encode()
    req = urllib.request.Request(cfg["base_url"].rstrip("/") + "/v1/images/edits", data=data,
        headers={"Authorization":"Bearer "+cfg["api_key"],"Content-Type":"multipart/form-data; boundary="+boundary},method="POST")
    try:
        with urllib.request.urlopen(req,timeout=240) as response: result=json.load(response)
    except urllib.error.HTTPError as exc:
        raise ImageGenerationError("Sub2API Images edit returned HTTP "+str(exc.code),status_code=502) from None
    items=result.get("data") or []
    if not items or not all(isinstance(x,dict) and x.get("b64_json") for x in items):
        raise ImageGenerationError("Sub2API returned no edited image data",status_code=502)
    data=format_image_result(_archive_items(items, body),request.prompt,request.response_format,int(time.time()))["data"]
    yield ImageOutput(kind="result",model=request.model,index=1,total=request.n,data=data)
