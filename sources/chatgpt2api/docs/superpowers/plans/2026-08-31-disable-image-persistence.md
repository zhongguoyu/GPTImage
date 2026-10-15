# Disable Image Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop server-side image generation results from being written to image storage or persisted in asynchronous task files while keeping image generation and editing responses usable.

**Architecture:** Keep the upstream image generation, polling, download, encoding, streaming, and task state machine unchanged. Make `format_image_result()` an in-memory formatter: return only Base64 data for `b64_json`, and a `data:` URL for `url`, without calling `ImageStorageService`. Keep task results in memory for the lifetime of the running service but omit the `data` field when serializing `image_tasks.json`.

**Tech Stack:** Python 3.13, FastAPI, `unittest`, `unittest.mock`, existing `ImageStorageService` and `ImageTaskService`.

---

### Task 1: Add failing formatter regression tests

**Files:**
- Create: `test/test_image_result_format.py`
- Reference: `services/protocol/conversation.py:265-293`

- [x] **Step 1: Write the failing tests**

Create the test module with two focused tests. Patch the storage class method rather than the conversation module singleton so the tests remain valid after the production import is removed:

```python
from __future__ import annotations

import base64
import unittest
from unittest import mock

from services.protocol.conversation import format_image_result


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


if __name__ == "__main__":
    unittest.main()
```

- [x] **Step 2: Run the tests and verify they fail for the old behavior**

Run:

```bash
uv run python -m unittest test.test_image_result_format -v
```

Expected: both tests fail because the current formatter calls `ImageStorageService.save()` and returns a generated `/images/...` URL. A test error caused by an import or assertion typo is not acceptable; fix the test until the failure is specifically about unwanted storage behavior.

### Task 2: Make image result formatting in-memory

**Files:**
- Modify: `services/protocol/conversation.py:16,151-153,265-293,1008-1014,1105-1110,1217-1221,1277-1282`
- Modify: `services/protocol/openai_v1_image_generations.py:21-31`
- Modify: `services/protocol/openai_v1_image_edit.py:61-73`
- Test: `test/test_image_result_format.py`

- [x] **Step 1: Remove the storage dependency and formatter save helper**

In `services/protocol/conversation.py`, remove:

```python
from services.image_storage_service import image_storage_service


def save_image_bytes(image_data: bytes, base_url: str | None = None) -> str:
    return image_storage_service.save(image_data, base_url).url
```

Also remove the storage-only `base_url` field from `ConversationRequest` and remove the corresponding `base_url` extraction and constructor arguments from both OpenAI image protocol handlers. Remove `request.base_url` from all four `format_image_result()` call sites.

- [x] **Step 2: Replace the formatter implementation with the minimal in-memory behavior**

Use this implementation for `format_image_result()`:

```python
def format_image_result(
    items: list[dict[str, Any]],
    prompt: str,
    response_format: str,
    created: int | None = None,
    message: str = "",
) -> dict[str, Any]:
    data: list[dict[str, Any]] = []
    for item in items:
        b64_json = str(item.get("b64_json") or "").strip()
        if not b64_json:
            continue
        revised_prompt = str(item.get("revised_prompt") or prompt).strip() or prompt
        if response_format == "b64_json":
            data.append({
                "b64_json": b64_json,
                "revised_prompt": revised_prompt,
            })
        else:
            data.append({
                "url": f"data:image/png;base64,{b64_json}",
                "revised_prompt": revised_prompt,
            })
    result: dict[str, Any] = {"created": created or int(time.time()), "data": data}
    if message and not data:
        result["message"] = message
    return result
```

- [x] **Step 3: Run the focused tests and verify they pass**

Run:

```bash
uv run python -m unittest test.test_image_result_format -v
```

Expected: `2` tests pass and no storage method is called.

- [x] **Step 4: Commit the formatter change**

```bash
git add services/protocol/conversation.py services/protocol/openai_v1_image_generations.py services/protocol/openai_v1_image_edit.py test/test_image_result_format.py
git commit -m "feat: stop persisting generated image results"
```

### Task 3: Add a failing asynchronous-task persistence test

**Files:**
- Modify: `test/test_image_task_service.py:1-10,89-104`
- Reference: `services/image_task_service.py:407-411`

- [x] **Step 1: Add a test proving current-process results remain available but are not serialized**

Add `base64` to the imports and add this test to `ImageTaskServiceTests`:

```python
    def test_success_task_keeps_result_in_memory_without_persisting_image_data(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            encoded = base64.b64encode(b"generated-image").decode("ascii")

            def handler(_payload):
                return {"data": [{"b64_json": encoded, "revised_prompt": "cat"}]}

            service = self.make_service(path, handler)
            service.submit_generation(
                OWNER,
                client_task_id="memory-only-result",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            current = wait_for_task(service, OWNER, "memory-only-result", "success")
            self.assertEqual(current["data"][0]["b64_json"], encoded)

            persisted = json.loads(path.read_text(encoding="utf-8"))
            persisted_task = persisted["tasks"][0]
            self.assertNotIn("data", persisted_task)

            reloaded = self.make_service(path, handler)
            reloaded_task = reloaded.list_tasks(OWNER, ["memory-only-result"])["items"][0]
            self.assertEqual(reloaded_task["status"], "success")
            self.assertNotIn("data", reloaded_task)
```

- [x] **Step 2: Run the new test and verify it fails because the old serializer writes `data`**

Run:

```bash
uv run python -m unittest test.test_image_task_service.ImageTaskServiceTests.test_success_task_keeps_result_in_memory_without_persisting_image_data -v
```

Expected: the test fails at `assertNotIn("data", persisted_task)` because `_save_locked()` currently serializes every task field.

- [x] **Step 3: Update the existing reload test's expectation**

In `test_success_task_persists_to_new_service_instance`, keep the assertions that the task reloads with `status == "success"`, but replace the assertion that reloaded task data contains the image URL with:

```python
            self.assertNotIn("data", result["items"][0])
```

This records the approved behavior that task metadata survives a restart while the image result does not.

### Task 4: Omit image result data from task-file serialization

**Files:**
- Modify: `services/image_task_service.py:407-411`
- Test: `test/test_image_task_service.py`

- [x] **Step 1: Serialize task metadata without the in-memory `data` field**

Replace `_save_locked()` with:

```python
    def _save_locked(self) -> None:
        tasks = sorted(self._tasks.values(), key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        items = [
            {key: value for key, value in task.items() if key != "data"}
            for task in tasks
        ]
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps({"tasks": items}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(self.path)
```

The task dictionary is intentionally left unchanged in memory, so `_public_task()` can still return the generated image to a caller while the process is running. Removing the whole `data` field also prevents a `data:` URL from being serialized as image content.

- [x] **Step 2: Run the asynchronous-task tests**

Run:

```bash
uv run python -m unittest test.test_image_task_service -v
```

Expected: all task-service tests pass, including the existing persistence and startup-recovery tests. The new test confirms the image result is available before restart and absent after reload.

- [x] **Step 3: Commit the task persistence change**

```bash
git add services/image_task_service.py test/test_image_task_service.py
git commit -m "fix: keep image task results out of task storage"
```

### Task 5: Run regression and full verification

**Files:**
- Verify: `services/protocol/conversation.py`
- Verify: `services/image_storage_service.py`
- Verify: `test/test_image_result_format.py`
- Verify: `test/test_image_task_service.py`
- Verify: `test/test_image_storage_service.py`

- [x] **Step 1: Run the focused image regression suite**

Run:

```bash
uv run python -m unittest \
  test.test_image_result_format \
  test.test_image_task_service \
  test.test_image_storage_service -v
```

Expected: all formatter, task, and existing storage tests pass. The storage tests must still prove that explicit historical local/WebDAV storage operations work.

- [x] **Step 2: Run the complete Python test discovery**

实际结果：完整发现命令已执行，但仓库当前测试环境包含需要外部服务、缺少 fixture 和既有断言不匹配的问题，未能全绿；与本次图片结果持久化改动相关的定向测试均通过。

Run:

```bash
uv run python -m unittest discover -s test -p 'test_*.py'
```

Expected: the test suite completes without failures. Tests that require a separately running upstream or local server should be reported with their environment prerequisite rather than silently omitted.

- [x] **Step 3: Check the final diff for accidental storage calls**

Run:

```bash
rg -n "save_image_bytes|image_storage_service\.save|ImageStorageService\.save" services/protocol services/image_task_service.py
git diff --check HEAD~2..HEAD
git status --short
```

Expected: no generation-path storage call remains; `git diff --check` reports no whitespace errors; only the intended implementation, tests, and already committed design/plan documents are present.
