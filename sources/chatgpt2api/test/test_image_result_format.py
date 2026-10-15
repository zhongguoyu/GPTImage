from __future__ import annotations

import base64
import unittest
from unittest import mock

from services.protocol.conversation import ConversationRequest, format_image_result, stream_image_outputs


IMAGE_B64 = base64.b64encode(b"image-bytes").decode("ascii")


class ImageResultFormatTests(unittest.TestCase):
    @mock.patch("services.image_storage_service.ImageStorageService.save")
    def test_b64_json_returns_image_data_without_storage_url(self, save_image):
        result = format_image_result(
            [{"b64_json": IMAGE_B64, "revised_prompt": "draw a poster"}],
            "poster",
            "b64_json",
            created=123,
        )

        self.assertEqual(result, {
            "created": 123,
            "data": [{
                "b64_json": IMAGE_B64,
                "revised_prompt": "draw a poster",
            }],
        })
        save_image.assert_not_called()

    @mock.patch("services.image_storage_service.ImageStorageService.save")
    def test_url_returns_data_url_without_storage_file(self, save_image):
        result = format_image_result(
            [{"b64_json": IMAGE_B64}],
            "poster",
            "url",
            created=456,
        )

        self.assertEqual(result, {
            "created": 456,
            "data": [{
                "url": f"data:image/png;base64,{IMAGE_B64}",
                "revised_prompt": "poster",
            }],
        })
        save_image.assert_not_called()

    @mock.patch("services.image_storage_service.ImageStorageService.save")
    def test_url_response_preserves_remote_url_without_inline_base64(self, save_image):
        result = format_image_result(
            [{"url": "https://cdn.example/image.png", "revised_prompt": "poster v2"}],
            "poster",
            "url",
            created=789,
        )

        self.assertEqual(result, {
            "created": 789,
            "data": [{
                "url": "https://cdn.example/image.png",
                "revised_prompt": "poster v2",
            }],
        })
        save_image.assert_not_called()

    def test_url_stream_result_does_not_download_image_bytes(self):
        class FakeBackend:
            def stream_conversation(self, **_kwargs):
                yield (
                    '{"conversation_id":"conv-1","message":{"author":{"role":"tool"},'
                    '"metadata":{"async_task_type":"image_gen"},'
                    '"content":{"content_type":"multimodal_text","parts":['
                    '{"content_type":"image_asset_pointer",'
                    '"asset_pointer":"file-service://file_00000000111111111111111111111111"}]}}}'
                )
                yield "[DONE]"

            def resolve_conversation_image_urls(self, conversation_id, file_ids, sediment_ids, poll_timeout_secs=None):
                self.resolved = (conversation_id, list(file_ids), list(sediment_ids), poll_timeout_secs)
                return ["https://cdn.example/generated.png"]

            def download_image_bytes(self, _urls):
                raise AssertionError("url responses must not download image bytes")

        backend = FakeBackend()
        outputs = list(stream_image_outputs(
            backend,
            ConversationRequest(
                prompt="poster",
                model="gpt-image-2",
                response_format="url",
            ),
        ))

        results = [output for output in outputs if output.kind == "result"]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].data, [{
            "url": "https://cdn.example/generated.png",
            "revised_prompt": "poster",
        }])
        self.assertEqual(backend.resolved[0], "conv-1")


if __name__ == "__main__":
    unittest.main()
