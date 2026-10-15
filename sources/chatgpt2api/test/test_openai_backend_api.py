from __future__ import annotations

import services.openai_backend_api as backend_module
from services.openai_backend_api import OpenAIBackendAPI


class ChunkedRawResponse:
    def __init__(self, chunks):
        self.headers = {"content-type": "text/event-stream"}
        self.status = 200
        self.chunks = list(chunks)
        self.read_calls = 0

    def __iter__(self):
        for chunk in self.chunks:
            self.read_calls += 1
            yield chunk

    def read(self, _size=-1):
        self.read_calls += 1
        return b"".join(self.chunks)


class JsonRawResponse:
    def __init__(self, payload):
        self.headers = {"content-type": "application/json"}
        self.status = 200
        self.payload = payload
        self.read_size = None

    def read(self, size=-1):
        self.read_size = size
        return self.payload


class ClosableJsonResponse:
    status_code = 200
    headers = {}

    def __init__(self):
        self.closed = False

    def json(self):
        return {"ok": True}

    def close(self):
        self.closed = True


def test_json_response_helper_closes_response_after_parsing():
    response = ClosableJsonResponse()
    backend = object.__new__(OpenAIBackendAPI)

    assert backend._read_json_response(response, "test") == {"ok": True}
    assert response.closed is True


def test_codex_sse_parser_yields_events_without_full_body_read():
    response = ChunkedRawResponse([
        b'data: {"type":"start"}\n\n',
        b'data: {"type":"image","id":"f1"}\n\n',
    ])

    events = list(OpenAIBackendAPI._iter_codex_response_events(response))

    assert events == [{"type": "start"}, {"type": "image", "id": "f1"}]
    assert response.read_calls == 2


def test_codex_sse_parser_discards_an_oversized_line_before_next_event(monkeypatch):
    monkeypatch.setattr(backend_module, "CODEX_SSE_BUFFER_LIMIT_BYTES", 32)
    response = ChunkedRawResponse([
        b"data: " + b"x" * 40,
        b"\n\n",
        b'data: {"type":"valid"}\n\n',
    ])

    events = list(OpenAIBackendAPI._iter_codex_response_events(response))

    assert events == [{"type": "valid"}]


def test_codex_sse_parser_accepts_large_image_event_with_configured_limit(monkeypatch):
    image = "data:image/png;base64," + "x" * 512
    payload = backend_module.json.dumps({"type": "image_generation_call", "result": image})
    monkeypatch.setattr(backend_module, "CODEX_SSE_BUFFER_LIMIT_BYTES", len(payload) + 16)
    response = ChunkedRawResponse([f"data: {payload}\n\n".encode()])

    events = list(OpenAIBackendAPI._iter_codex_response_events(response))

    assert events == [{"type": "image_generation_call", "result": image}]


def test_codex_sse_parser_drops_an_unterminated_oversized_line(monkeypatch):
    monkeypatch.setattr(backend_module, "CODEX_SSE_BUFFER_LIMIT_BYTES", 32)
    response = ChunkedRawResponse([
        b"data: " + b"x" * 40,
        b'\ndata: {"type":"valid"}\n\n',
    ])

    events = list(OpenAIBackendAPI._iter_codex_response_events(response))

    assert events == [{"type": "valid"}]


def test_codex_json_parser_allows_large_payload_beyond_debug_preview_limit(monkeypatch):
    monkeypatch.setattr(backend_module, "CODEX_RESPONSE_DEBUG_LIMIT_BYTES", 16)
    payload = backend_module.json.dumps({"type": "image_generation_call", "result": "x" * 100}).encode()
    response = JsonRawResponse(payload)

    events = list(OpenAIBackendAPI._iter_codex_response_events(response))

    assert events[0]["result"] == "x" * 100
    assert response.read_size == backend_module.CODEX_JSON_RESPONSE_LIMIT_BYTES + 1
