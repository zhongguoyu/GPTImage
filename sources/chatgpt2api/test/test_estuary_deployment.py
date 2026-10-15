from services.protocol.conversation import image_result_items_from_urls
class Backend:
    def download_image_bytes(self, urls):
        self.urls = urls
        return [b"image-bytes"]
def test_estuary_url_materialized_with_originating_backend():
    b = Backend()
    urls = ["https://chatgpt.com/backend-api/estuary/content?id=test"]
    assert image_result_items_from_urls(b, urls, "url") == [{"b64_json": "aW1hZ2UtYnl0ZXM="}]
    assert b.urls == urls
def test_public_url_preserved():
    assert image_result_items_from_urls(object(), ["https://example.com/image.png"], "url") == [{"url": "https://example.com/image.png"}]
