from __future__ import annotations

import asyncio
import base64
from unittest import mock

import pytest
from fastapi import HTTPException

from api import image_inputs
from utils import helper


def test_read_image_sources_rejects_request_aggregate_budget():
    budget = image_inputs.ImageInputBudget(max_bytes=10)
    sources = [(b"123456", "one.png", "image/png"), (b"123456", "two.png", "image/png")]

    with pytest.raises(HTTPException) as raised:
        asyncio.run(image_inputs.read_image_sources(sources, budget=budget))

    assert raised.value.status_code == 413
    assert raised.value.detail["error"] == "total image input size exceeds limit"


def test_single_image_limit_remains_enforced(monkeypatch):
    monkeypatch.setattr(image_inputs, "MAX_IMAGE_REFERENCE_BYTES", 10)

    with pytest.raises(HTTPException) as raised:
        image_inputs._decode_base64_image(
            base64.b64encode(b"x" * 11).decode("ascii"),
            "image.png",
            "image/png",
        )

    assert raised.value.status_code == 400


def test_inline_json_sources_reserve_shared_budget(monkeypatch):
    budget = image_inputs.ImageInputBudget(max_bytes=10)
    encoded = base64.b64encode(b"123456").decode("ascii")

    with pytest.raises(HTTPException) as raised:
        image_inputs._sources_from_value([encoded, encoded], budget)

    assert raised.value.status_code == 413
    assert budget.remaining_bytes == 4


def test_image_input_budget_handles_the_same_bytes_object_more_than_once():
    payload = b"same-payload"
    budget = image_inputs.ImageInputBudget(max_bytes=len(payload) * 2)

    budget.reserve(payload)
    budget.reserve(payload)
    budget.consume(payload)
    budget.consume(payload)

    assert budget.used_bytes == len(payload) * 2


def test_json_image_rejects_oversized_base64_before_decoding(monkeypatch):
    monkeypatch.setattr(helper, "MAX_JSON_IMAGE_BYTES", 3)
    encoded = base64.b64encode(b"1234").decode("ascii")

    with mock.patch.object(helper.base64, "b64decode", side_effect=AssertionError("must not decode")):
        with pytest.raises(HTTPException) as raised:
            helper._decode_json_image_string(encoded, 1)

    assert raised.value.status_code == 400
    assert raised.value.detail["error"] == "image file is too large"


def test_message_remote_image_uses_bounded_raw_reader_and_closes_response(monkeypatch):
    class RawReader:
        def __init__(self, payload: bytes):
            self.payload = payload

        def read(self, size: int = -1) -> bytes:
            if not self.payload:
                return b""
            if size < 0:
                size = len(self.payload)
            chunk, self.payload = self.payload[:size], self.payload[size:]
            return chunk

    class Response:
        status_code = 200
        headers = {"content-type": "image/png"}

        def __init__(self):
            self.raw = RawReader(b"image-bytes")
            self.closed = False

        @property
        def content(self):
            raise AssertionError("fallback must use bounded raw reads")

        def close(self):
            self.closed = True

    response = Response()
    monkeypatch.setattr(image_inputs.requests, "get", lambda *args, **kwargs: response)
    budget = image_inputs.ImageInputBudget(max_bytes=len(b"image-bytes"))

    result = helper._decode_message_image_url("https://example.test/image.png", budget)

    assert result == (b"image-bytes", "image/png")
    assert response.closed is True
