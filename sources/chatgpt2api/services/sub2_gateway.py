"""Strict Sub2API gateway mode. No ChatGPT Web fallback is permitted."""
from __future__ import annotations
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from fastapi import HTTPException

def enabled():
    from services.routing_mode import sub2_enabled
    return sub2_enabled()

def settings():
    path = os.environ.get("IMAGE_UPSTREAM_CONFIG")
    if not path:
        raise HTTPException(503, detail={"error": "Sub2API upstream configuration is missing"})
    cfg = json.loads(Path(path).read_text())
    if not cfg.get("base_url") or not cfg.get("api_key"):
        raise HTTPException(503, detail={"error": "Sub2API upstream configuration is incomplete"})
    return cfg

def unsupported(feature):
    raise HTTPException(501, detail={"error": feature + " 在当前 Sub2API 模式没有对应接口；不会回退到 ChatGPT 网页。"})

def model_name(value):
    model = str(value or "auto")
    if model == "auto":
        return settings().get("text_model", "gpt-5.6")
    return {"gpt-5-6": "gpt-5.6"}.get(model, model)

def _open(path, body=None):
    cfg = settings()
    req = urllib.request.Request(cfg["base_url"].rstrip("/") + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + cfg["api_key"], "Content-Type": "application/json"})
    try:
        # Same request timeout as the existing Images gateway.
        return urllib.request.urlopen(req, timeout=240)
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
        raise HTTPException(status, detail={"error": f"Sub2API {path} returned HTTP {status}"}) from None
    except (urllib.error.URLError, TimeoutError):
        raise HTTPException(502, detail={"error": "Sub2API connection failed"}) from None

def _check(data):
    if not isinstance(data, dict):
        raise HTTPException(502, detail={"error": "Invalid Sub2API response"})
    if data.get("error") or data.get("type") in {"error", "response.failed", "response.incomplete"}:
        raise HTTPException(502, detail={"error": "Sub2API request failed or was incomplete"})
    if data.get("status") in {"failed", "incomplete"}:
        raise HTTPException(502, detail={"error": "Sub2API request did not complete"})
    return data

def json_request(path, body=None):
    with _open(path, body) as response:
        return _check(json.load(response))

def _events(path, body):
    with _open(path, body) as response:
        content_type = response.headers.get("Content-Type", "")
        if "text/event-stream" not in content_type:
            raise HTTPException(502, detail={"error": "Sub2API did not return an event stream"})
        data_lines = []
        terminal = False
        for raw in response:
            line = raw.decode("utf-8").rstrip("\r\n")
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif not line and data_lines:
                payload = "\n".join(data_lines);data_lines=[]
                if payload == "[DONE]":
                    terminal = True
                    break
                event = _check(json.loads(payload))
                if event.get("type") in {"response.completed", "message_stop"}:
                    terminal = True
                if any(x.get("finish_reason") for x in event.get("choices", []) if isinstance(x, dict)):
                    terminal = True
                yield event
        if not terminal:
            raise HTTPException(502, detail={"error": "Sub2API stream ended before completion"})

def _archive_response(result):
    # Keep the gallery populated for native Responses image tools too.
    from services.protocol.image_upstream import _archive_items
    for item in result.get("output", []):
        if item.get("type") == "image_generation_call" and item.get("result"):
            _archive_items([{"b64_json": item["result"]}], {})

def _response_events(payload):
    for event in _events("/v1/responses", payload):
        if event.get("type") == "response.completed":
            _archive_response(event.get("response") or {})
        yield event

def forward(path, body):
    payload = {k:v for k,v in body.items() if v is not None and not k.startswith("_")}
    payload["model"] = model_name(payload.get("model"))
    if path == "/v1/chat/completions" and not payload.get("messages") and payload.get("prompt"):
        payload["messages"] = [{"role":"user", "content":payload.pop("prompt")}]
    from services.config import config
    system_prompt = config.global_system_prompt
    if system_prompt:
        if path == "/v1/chat/completions":
            payload["messages"] = [{"role":"system","content":system_prompt}] + list(payload.get("messages") or [])
        elif path == "/v1/responses":
            payload["instructions"] = system_prompt + "\n" + str(payload.get("instructions") or "")
        elif path == "/v1/messages":
            existing = payload.get("system")
            payload["system"] = ([{"type":"text","text":system_prompt}] + existing) if isinstance(existing, list) else system_prompt + "\n" + str(existing or "")
    if payload.get("stream"):
        return _response_events(payload) if path == "/v1/responses" else _events(path, payload)
    result = json_request(path, payload)
    if path == "/v1/responses":
        _archive_response(result)
    return result

def search(prompt):
    result = forward("/v1/responses", {"model":"auto", "input":prompt,
        "tools":[{"type":"web_search"}], "tool_choice":"required", "stream":False})
    answer=[];sources=[]
    for item in result.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                answer.append(content.get("text", ""))
                for citation in content.get("annotations", []):
                    if citation.get("type") == "url_citation":
                        sources.append({"title":citation.get("title", ""),"url":citation.get("url", ""),"snippet":""})
    return {"answer":"\n".join(answer), "sources":sources}
