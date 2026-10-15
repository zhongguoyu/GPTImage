# Runtime Memory Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound long-lived Python/native allocations and reduce transient image/task peaks while preserving the configured scheduler concurrency and synchronous upstream behavior.

**Architecture:** Add bounded cleanup at ownership boundaries: compact scheduler heaps, TTL-bound progress/aliases, byte-bound response cache, aggregate image budgets, incremental SSE parsing, single-pass image editing, and bounded file-task admission. Keep hot paths O(log n) or streaming, and expose cleanup/byte metrics for verification.

**Tech Stack:** Python 3.13, FastAPI, asyncio, `concurrent.futures`, Pillow, stdlib streaming/JSON APIs, SQLite, pytest.

---

## File map

- Modify `services/scheduler/account_pool.py`: reclaim stale capability-heap entries by threshold-triggered rebuild.
- Modify `services/account_service.py`: TTL-bound refresh/re-login progress, token alias retention, and compact result summaries.
- Modify `services/protocol/chat_completion_cache.py`: enforce process-level byte budget and bypass large responses.
- Modify `services/config.py`: expose memory/input/task limits with validated defaults.
- Modify `api/image_inputs.py`: enforce per-request aggregate image bytes and avoid duplicate input copies.
- Modify `services/openai_backend_api.py`: parse SSE incrementally without retaining the full raw response/event list.
- Modify `services/protocol/conversation.py`: consume image events incrementally and release raw input/output buffers early.
- Modify `services/protocol/openai_v1_image_edit.py`: compose masks once and pass token metadata forward.
- Modify `services/editable_file_task_service.py`: bounded executor, admission semaphore, disk spooling, and terminal-task cleanup.
- Modify `services/image_task_service.py`: use bounded task cleanup/recovery from the shared state store.
- Modify `services/log_store.py`: use controlled SQLite connections and explicit close/PRAGMA configuration.
- Modify `services/log_service.py`: close log store at application shutdown and preserve necessary log semantics.
- Modify `services/storage/factory.py`: lazy-load optional storage dependencies.
- Modify `services/image_storage_service.py`: close WebDAV sessions after operation/sync lifetimes.
- Modify `services/backup_service.py`: write archives through a temporary file/stream instead of retaining duplicate `BytesIO` copies.
- Modify `api/app.py`: wire cleanup and shutdown hooks.
- Create `test/test_image_inputs.py`: aggregate image-input budget tests.
- Create `test/test_openai_backend_api.py`: incremental Codex/SSE parser tests.
- Create `test/test_conversation.py`: incremental image-event consumer tests.
- Create `test/test_openai_v1_image_edit.py`: single mask-composition tests.
- Create `test/test_editable_file_task_service.py`: bounded admission and spool cleanup tests.
- Create `test/test_memory_regressions.py`: deterministic heap/cache bound tests.
- Extend `test/test_scheduler_account_pool.py` for heap compaction.
- Create `test/test_account_service.py` for progress and alias bounds.
- Extend `test/test_chat_completion_cache.py` for byte eviction.
- Extend `test/test_log_store.py` and `test/test_image_storage_service.py` for resource lifecycle cleanup.

### Task 1: Reclaim stale scheduler heap entries

**Files:**
- Modify: `services/scheduler/account_pool.py`
- Test: `test/test_scheduler_account_pool.py`

- [ ] **Step 1: Write the failing regression test**

```python
def test_repeated_lease_release_rebuilds_stale_capability_heaps():
    pool = make_pool_with_accounts(2)

    requirement = AccountRequirement(Lane.TEXT, "auto", cost=1)
    for _ in range(5000):
        lease = pool.try_acquire(requirement, deadline=pool.clock() + 5)
        assert lease is not None
        pool.release(lease, outcome=Outcome.SUCCESS, latency_ms=1)

    assert pool.debug_heap_entry_count() <= pool.account_count * 4
    assert pool.debug_stale_heap_entry_count() <= pool.account_count
```

- [ ] **Step 2: Run the test and verify stale entries grow without the fix**

Run: `uv run pytest test/test_scheduler_account_pool.py::test_repeated_lease_release_rebuilds_stale_capability_heaps -q`

Expected: FAIL because the current heap contains stale tuples from every publish/unpublish cycle.

- [ ] **Step 3: Add threshold-triggered heap rebuilding**

After every publish/unpublish, check only cheap counters. When a heap exceeds `max(64, live_accounts * 8)` and stale entries exceed half of the heap, rebuild from the current account revision/live-capability map:

```python
def _maybe_rebuild_heap_locked(self, capability: str) -> None:
    heap = self._capability_heaps[capability]
    live = self._live_accounts_for_capability_locked(capability)
    if len(heap) < max(64, len(live) * 8):
        return
    rebuilt = [
        entry for entry in heap
        if self._entry_is_current_locked(capability, entry)
        and entry.account_key in live
        and entry.account_key not in self._leases_by_account
    ]
    heap[:] = rebuilt
    heapq.heapify(heap)
```

Do not scan/remove one arbitrary heap item from the request path. Increment `stale`/`live` counters while validating top entries so rebuilds are amortized.

- [ ] **Step 4: Run scheduler and memory regression tests**

Run: `uv run pytest test/test_scheduler_account_pool.py test/test_scheduler_service.py test/test_scheduler_reconciler.py -q`

Expected: all scheduler tests pass, and the new heap bound test passes.

- [ ] **Step 5: Commit heap compaction**

```bash
git add services/scheduler/account_pool.py test/test_scheduler_account_pool.py
git commit -m "perf: compact stale scheduler heap entries"
```

### Task 2: Bound progress snapshots and token aliases

**Files:**
- Modify: `services/account_service.py`
- Modify: `services/config.py`
- Test: account refresh/relogin tests

- [ ] **Step 1: Write TTL and compact-result tests**

```python
def test_finished_refresh_progress_drops_full_account_snapshot(monkeypatch, service):
    service.init_refresh_progress("p1", 2)
    service.finish_refresh_progress("p1", {
        "success": 2,
        "failed": 0,
        "items": [{"access_token": "secret", "email": "a@example.com"}],
    })

    result = service.get_refresh_progress("p1")

    assert "items" not in result["result"]
    assert result["result"]["success"] == 2


def test_expired_aliases_are_removed(monkeypatch, service):
    service._token_aliases = {"old": "new"}
    monkeypatch.setattr(time, "time", lambda: 2_000)
    service._token_alias_expiry = {"old": 1_000}

    service.cleanup_runtime_metadata()

    assert service._token_aliases == {}
```

- [ ] **Step 2: Run focused tests and verify the current full result remains**

Run: `uv run pytest test/test_account_service.py -q`

Expected: the compact result assertion fails because completed results currently retain the full `items` snapshot.

- [ ] **Step 3: Add validated TTL/count settings and cleanup**

Add `progress_ttl_seconds=900`, `token_alias_ttl_seconds=86400`, and `token_alias_max_per_account=4` to `ConfigStore`. On token rotation, record expiry and prune the oldest aliases for that account. On progress completion, replace account lists with counts and errors; run `cleanup_runtime_metadata()` from the existing account watcher and before serving progress reads.

```python
def cleanup_runtime_metadata(self, now: float | None = None) -> None:
    current = time.time() if now is None else now
    with self._refresh_progress_lock:
        self._refresh_progress = {
            key: value for key, value in self._refresh_progress.items()
            if current - float(value.get("updated_ts") or current) <= config.progress_ttl_seconds
        }
    with self._lock:
        expired = [
            alias for alias, expires_at in self._token_alias_expiry.items()
            if expires_at <= current
        ]
        for alias in expired:
            self._token_aliases.pop(alias, None)
            self._token_alias_expiry.pop(alias, None)
```

- [ ] **Step 4: Preserve API status fields and run regression tests**

Run: `uv run pytest test/test_account_service.py test/test_account_image_capabilities.py test/test_scheduler_reconciler.py -q`

Expected: refresh/re-login progress status, token authentication, and account capability behavior remain green while secrets/full snapshots are absent from finished progress objects.

- [ ] **Step 5: Commit bounded runtime metadata**

```bash
git add services/account_service.py services/config.py test/test_account_service.py
git commit -m "perf: bound account progress and token alias memory"
```

### Task 3: Add a byte budget to the response cache

**Files:**
- Modify: `services/protocol/chat_completion_cache.py`
- Modify: `services/config.py`
- Test: `test/test_chat_completion_cache.py`

- [ ] **Step 1: Write byte-budget and eviction tests**

```python
def test_cache_evicts_by_bytes_before_entry_count(tmp_path):
    cache = ChatCompletionCache()
    config.data["chat_completion_cache"] = {"enabled": True, "ttl_seconds": 60, "max_entries": 100, "max_bytes": 32}
    cache.set("a", {"text": "x" * 24})
    cache.set("b", {"text": "y" * 24})

    assert cache.get("a") is None
    assert cache.get("b") is not None
    assert cache.current_bytes <= 32


def test_large_response_is_not_retained():
    cache = ChatCompletionCache()
    config.data["chat_completion_cache"] = {"enabled": True, "ttl_seconds": 60, "max_entries": 100, "max_bytes": 64}

    cache.set("large", {"text": "z" * 1024})

    assert cache.get("large") is None
    assert cache.current_bytes == 0
```

- [ ] **Step 2: Run the tests and verify the cache has no byte bound**

Run: `uv run pytest test/test_chat_completion_cache.py -q`

Expected: the new tests fail because current eviction is entry-count/TTL based only.

- [ ] **Step 3: Track serialized size and evict under a byte budget**

Add `CacheEntry.size_bytes`, `self._current_bytes`, and `max_bytes`. Estimate once on insertion using compact JSON serialization; reject an entry larger than the budget. Evict expired entries and least-recent entries until both limits hold. For stream inflight data, keep the current owner’s chunks only until completion and never insert an over-budget response into the persistent cache.

```python
def _put_locked(self, key: str, value: object) -> None:
    size = _estimate_json_bytes(value)
    if size > self.max_bytes:
        self._entries.pop(key, None)
        return
    previous = self._entries.pop(key, None)
    if previous is not None:
        self._current_bytes -= previous.size_bytes
    self._entries[key] = CacheEntry(value=value, size_bytes=size, expires_at=time.time() + self.ttl)
    self._current_bytes += size
    while len(self._entries) > self.max_entries or self._current_bytes > self.max_bytes:
        _, evicted = self._entries.popitem(last=False)
        self._current_bytes -= evicted.size_bytes
```

- [ ] **Step 4: Add cache metrics and run cache/protocol tests**

Run: `uv run pytest test/test_chat_completion_cache.py test/test_text_model_routing.py -q`

Expected: cache behavior, deduplication, TTL, and streaming tests pass; `current_bytes` never exceeds the configured budget.

- [ ] **Step 5: Commit the byte-bounded cache**

```bash
git add services/protocol/chat_completion_cache.py services/config.py test/test_chat_completion_cache.py
git commit -m "perf: bound response cache by resident bytes"
```

### Task 4: Bound image input materialization

**Files:**
- Modify: `api/image_inputs.py`
- Modify: `services/config.py`
- Modify: `api/image_tasks.py` and `api/ai.py`
- Test: `test/test_image_inputs.py`

- [ ] **Step 1: Write aggregate-budget tests**

```python
def test_read_image_sources_rejects_aggregate_budget(monkeypatch):
    monkeypatch.setattr(config, "image_input_max_bytes", 10)

    with pytest.raises(HTTPException, match="total image input size"):
        asyncio.run(read_image_sources([data_url(b"123456"), data_url(b"123456")]))


def test_single_image_limit_remains_enforced(monkeypatch):
    monkeypatch.setattr(image_inputs, "MAX_IMAGE_REFERENCE_BYTES", 10)
    with pytest.raises(HTTPException, match="50MB"):
        image_inputs._decode_base64_image(base64.b64encode(b"x" * 11).decode(), "x.png", "image/png")
```

- [ ] **Step 2: Run the image-input tests and verify only per-image checks exist**

Run: `uv run pytest test/test_image_inputs.py -q`

Expected: aggregate-budget test fails before the shared byte counter is added.

- [ ] **Step 3: Implement one request-scoped byte counter**

Add `ImageInputBudget(max_bytes)` and pass it through Base64, upload, URL, and mask reads. Increment after each source is materialized; raise the existing 4xx-mapped input exception before adding a source that would exceed the budget. Use `response.iter_bytes()`/bounded reads for URL inputs where supported and close responses in `finally`.

```python
@dataclass
class ImageInputBudget:
    max_bytes: int
    used_bytes: int = 0

    def consume(self, payload: bytes) -> bytes:
        next_total = self.used_bytes + len(payload)
        if next_total > self.max_bytes:
            raise HTTPException(status_code=413, detail={"error": "total image input size exceeds limit"})
        self.used_bytes = next_total
        return payload
```

- [ ] **Step 4: Release duplicate references at the protocol boundary**

After `ConversationRequest` owns the encoded request images, clear temporary raw lists and source buffers. Keep the existing per-image 50 MiB validation and add a config getter with default 128 MiB.

- [ ] **Step 5: Run image endpoint and edit tests**

Run: `uv run pytest test/test_image_inputs.py test/test_v1_images_edits.py test/test_image_task_service.py -q`

Expected: valid multi-image requests still pass; requests over the aggregate cap fail before the upstream call.

- [ ] **Step 6: Commit image input bounds**

```bash
git add api/image_inputs.py services/config.py api services/protocol test/test_image_inputs.py test/test_v1_images_edits.py
git commit -m "perf: bound materialized image input bytes"
```

### Task 5: Parse Codex image SSE incrementally

**Files:**
- Modify: `services/openai_backend_api.py`
- Modify: `services/protocol/conversation.py`
- Create: `test/test_openai_backend_api.py`
- Create: `test/test_conversation.py`
- Test: `test/test_image_task_service.py`, `test/test_scheduler_image_integration.py`

- [ ] **Step 1: Write an incremental parser test**

```python
def test_codex_sse_parser_does_not_require_full_body():
    chunks = [b"data: {\"type\":\"start\"}\n\n", b"data: {\"type\":\"image\",\"id\":\"f1\"}\n\n"]
    response = ChunkedRawResponse(chunks)

    events = list(OpenAIBackendAPI._iter_codex_response_events(response))

    assert events == [{"type": "start"}, {"type": "image", "id": "f1"}]
    assert response.read_calls > 2
```

- [ ] **Step 2: Run the parser test and verify the current implementation reads all bytes**

Run: `uv run pytest test/test_openai_backend_api.py::test_codex_sse_parser_does_not_require_full_body -q`

Expected: FAIL because `_iter_codex_response_events` currently calls `response.read()` and constructs a complete event list.

- [ ] **Step 3: Replace full-body parsing with line/event iteration**

Accumulate only the current SSE `data:` lines, parse at the blank-line boundary with `json.loads`, and yield immediately. For JSON error responses, use one bounded body read capped at the configured error size. Debug logging records event counts and the first small event type, never raw image data or the full body.

```python
def _iter_codex_response_events(response):
    data_lines: list[str] = []
    for raw_line in response:
        line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
            continue
        if line or not data_lines:
            continue
        payload = "\n".join(data_lines)
        data_lines.clear()
        if payload == "[DONE]":
            continue
        event = json.loads(payload)
        if isinstance(event, dict):
            yield event
    if data_lines:
        event = json.loads("\n".join(data_lines))
        if isinstance(event, dict):
            yield event
```

- [ ] **Step 4: Remove list aggregation in the image conversation path**

Replace `list(_iter_codex_response_events(...))` with a generator consumer that extracts file IDs, image URLs, and terminal errors as events arrive. Keep only the bounded output metadata required by the existing response formatter; do not retain raw event dictionaries after each event.

- [ ] **Step 5: Run backend and image integration tests**

Run: `uv run pytest test/test_openai_backend_api.py test/test_conversation.py test/test_image_task_service.py test/test_scheduler_image_integration.py -q`

Expected: SSE parsing, image result ordering, polling, stream/non-stream responses, and error handling pass.

- [ ] **Step 6: Commit incremental SSE processing**

```bash
git add services/openai_backend_api.py services/protocol/conversation.py test/test_openai_backend_api.py test/test_conversation.py
git commit -m "perf: stream codex image event parsing"
```

### Task 6: Compose image-edit masks once

**Files:**
- Modify: `services/protocol/openai_v1_image_edit.py`
- Modify: `services/protocol/conversation.py`
- Create: `test/test_openai_v1_image_edit.py`
- Test: `test/test_v1_images_edits.py`

- [ ] **Step 1: Write the single-composition regression test**

```python
def test_image_edit_reuses_composited_mask(monkeypatch, edit_request):
    calls = 0
    original = edit_module._composite_mask

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(edit_module, "_composite_mask", counted)
    body = build_request(edit_request)
    format_outputs([{"url": "https://example/image.png"}], body)

    assert calls == 1
```

- [ ] **Step 2: Run the test and verify the mask is composed twice**

Run: `uv run pytest test/test_openai_v1_image_edit.py -q`

Expected: FAIL because request construction and output formatting each composite the mask.

- [ ] **Step 3: Cache only the prepared mask metadata across protocol stages**

Have `build_request` compose once, encode the prepared image payload, and attach `_input_image_tokens`/prepared output metadata to the request context. `format_outputs` reads that metadata and never reopens or recomposes the source mask. Clear raw `images`/`mask` fields immediately after the request payload is built.

- [ ] **Step 4: Run edit, quota, and protocol tests**

Run: `uv run pytest test/test_openai_v1_image_edit.py test/test_v1_images_edits.py test/test_quota_service.py -q`

Expected: output shape, token accounting, quota units, and image edit behavior remain unchanged with one composition.

- [ ] **Step 5: Commit single-pass edit preparation**

```bash
git add services/protocol/openai_v1_image_edit.py services/protocol/conversation.py test/test_openai_v1_image_edit.py
git commit -m "perf: avoid duplicate image edit mask composition"
```

### Task 7: Bound editable-file task execution and payload lifetime

**Files:**
- Modify: `services/editable_file_task_service.py`
- Modify: `services/config.py`
- Modify: `api/ai.py` to return the bounded-task admission status
- Test: `test/test_editable_file_task_service.py`

- [ ] **Step 1: Write admission, thread-count, and spool cleanup tests**

```python
def test_file_tasks_use_bounded_admission(tmp_path):
    service = EditableFileTaskService(tmp_path / "tasks.json", max_workers=1, max_pending=1)

    first = service.submit("ppt", ["a"], slow_handler)
    second = service.submit("ppt", ["b"], slow_handler)
    third = service.submit("ppt", ["c"], slow_handler)

    assert first["status"] in {"queued", "running"}
    assert second["status"] == "queued"
    assert third["status"] == "rejected"
    assert service.worker_thread_count <= 1


def test_spooled_base64_input_is_removed_after_task(tmp_path):
    service = EditableFileTaskService(tmp_path / "tasks.json", spool_dir=tmp_path / "spool")
    task = service.submit("ppt", ["a" * 10000], successful_handler)
    service.wait(task["id"])

    assert list((tmp_path / "spool").iterdir()) == []
```

- [ ] **Step 2: Run the task tests and verify one-thread-per-task behavior**

Run: `uv run pytest test/test_editable_file_task_service.py -q`

Expected: the admission/thread-count test fails before the executor and semaphore are added.

- [ ] **Step 3: Use an executor and disk spool for queued payloads**

Create `ThreadPoolExecutor(max_workers=config.file_task_max_workers)` and a `BoundedSemaphore(max_pending + max_workers)`. Acquire before submission; return a rejected task record when unavailable. Write each Base64 source to a temporary file, pass paths to the worker, and unlink files in a `finally` block. Release the semaphore in the future callback. Keep the public task record metadata-only.

```python
if not self._admission.acquire(blocking=False):
    return self._create_rejected_task(key, "file task queue is full")
paths = self._spool_inputs(base64_images)
future = self._executor.submit(self._run_task_from_spool, key, kind, paths, identity, base_url)
future.add_done_callback(lambda _: self._admission.release())
```

- [ ] **Step 4: Add terminal TTL/max-count cleanup**

On submit, list, and startup, delete terminal tasks older than seven days and then oldest terminal rows over 10,000. Use the shared task store in production; preserve the JSON adapter for explicit rollback/test fixtures.

- [ ] **Step 5: Run file-task and API tests**

Run: `uv run pytest test/test_editable_file_task_service.py test/test_gpt_ppt.py test/test_gpt_psd.py -q`

Expected: task creation, polling, export, failure cleanup, rejection status, and API responses pass without unbounded threads or retained Base64 queues.

- [ ] **Step 6: Commit bounded file-task execution**

```bash
git add services/editable_file_task_service.py services/config.py api/ai.py test/test_editable_file_task_service.py
git commit -m "perf: bound editable file task workers and payloads"
```

### Task 8: Reduce SQLite/log/client/backup lifetime overhead

**Files:**
- Modify: `services/log_store.py`
- Modify: `services/log_service.py`
- Modify: `services/storage/factory.py`
- Modify: `services/image_storage_service.py`
- Modify: `services/backup_service.py`
- Modify: `api/app.py`
- Test: `test/test_log_store.py`, `test/test_log_service.py`, `test/test_image_storage_service.py`, `test/test_backup_service.py`

- [ ] **Step 1: Write lifecycle tests**

```python
def test_log_store_close_releases_connection(tmp_path):
    store = SQLiteLogStore(tmp_path / "logs.db", tmp_path / "logs.jsonl")
    store.initialize()
    store.add("info", "ok", {})
    connection = store._connection()

    store.close()

    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")


def test_webdav_operation_closes_client(monkeypatch):
    service = ImageStorageService()
    client = RecordingWebDAVClient()
    monkeypatch.setattr("services.image_storage_service.WebDAVClient", lambda *_: client)
    monkeypatch.setattr(service, "mode", lambda: "webdav")

    service.save(b"data")

    assert client.closed is True
```

- [ ] **Step 2: Run lifecycle tests and verify retained clients/connections**

Run: `uv run pytest test/test_log_store.py test/test_image_storage_service.py test/test_backup_service.py -q`

Expected: the new close assertions fail before explicit lifecycle handling is implemented.

- [ ] **Step 3: Bound log SQLite connections and close at shutdown**

Use one controlled connection per log store with a lock, `check_same_thread=False`, `busy_timeout=5000`, WAL, `synchronous=NORMAL`, and negative `cache_size`. Add `close()` and call it in the `api/app.py` lifespan `finally` block after retention workers stop. Query methods still cap page size at 100.

- [ ] **Step 4: Close WebDAV sessions in every operation**

Wrap ordinary `save/get/delete` client creation in `try/finally: client.close()`. Wrap `sync_all` in the same lifetime boundary and close response streams before returning. Do not alter local-storage behavior.

```python
client = WebDAVClient(self.settings())
try:
    remote_url = client.put(rel, image_data)
finally:
    client.close()
```

- [ ] **Step 5: Stream backup archive creation through a temporary file**

Replace the in-memory archive plus `getvalue()` path with `tempfile.NamedTemporaryFile`, pass a file handle to encryption/upload, and unlink it after completion. Preserve archive member names and backup state updates.

- [ ] **Step 6: Run lifecycle and full service tests**

Run: `uv run pytest test/test_log_store.py test/test_log_service.py test/test_image_storage_service.py test/test_backup_service.py -q`

Expected: all tests pass, including retention, backup contents, WebDAV calls, and shutdown behavior.

- [ ] **Step 7: Commit lifecycle/resource cleanup**

```bash
git add services/log_store.py services/log_service.py services/storage/factory.py services/image_storage_service.py services/backup_service.py api/app.py test/test_log_store.py test/test_log_service.py test/test_image_storage_service.py test/test_backup_service.py
git commit -m "perf: bound sqlite and external resource lifetimes"
```

### Task 9: Verify memory bounds, throughput, and complete regression coverage

**Files:**
- Create: `test/test_memory_regressions.py`
- Modify: `docs/deployment.md`

- [ ] **Step 1: Add deterministic memory-bound tests**

```python
def test_account_pool_memory_does_not_grow_with_reuse():
    pool = make_pool_with_accounts(4)
    requirement = AccountRequirement(Lane.TEXT, "auto", cost=1)
    for _ in range(10000):
        lease = pool.try_acquire(requirement, deadline=pool.clock() + 5)
        assert lease is not None
        pool.release(lease, outcome=Outcome.SUCCESS, latency_ms=1)
    assert pool.debug_heap_entry_count() < 4 * 32


def test_response_cache_resident_bytes_are_bounded():
    cache = ChatCompletionCache()
    config.data["chat_completion_cache"] = {"enabled": True, "ttl_seconds": 60, "max_entries": 1000, "max_bytes": 1024}
    for index in range(100):
        cache.set(str(index), {"text": "x" * 200})
    assert cache.current_bytes <= 1024
```

- [ ] **Step 2: Run focused memory tests**

Run: `uv run pytest test/test_memory_regressions.py test/test_scheduler_account_pool.py test/test_chat_completion_cache.py -q`

Expected: exit code 0; heap/cache metrics remain within their configured bounds.

- [ ] **Step 3: Run the full suite and collect a startup baseline**

Run: `uv run pytest -q`

Expected: exit code 0.

Run: `uv run python -c "import resource; import main; print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)"`

Expected: successful import and a recorded RSS baseline for comparison with the pre-change measurement; no assertion of an absolute RSS value is made in the test.

- [ ] **Step 4: Verify unchanged scheduler concurrency and performance-sensitive paths**

Run: `uv run pytest test/test_scheduler_service.py test/test_scheduler_text_integration.py test/test_scheduler_image_integration.py test/test_quota_service.py -q`

Expected: configured concurrency remains text 128, image generation 32, image edit 32; lease fairness, quota reservation, request ordering, and upstream timeout behavior remain green.

- [ ] **Step 5: Perform inline code review and check the branch**

Run: `git diff feat/high-performance-scheduler...HEAD --check`

Run: `git status --short --branch`

Expected: no whitespace errors; branch is `feat/memory-optimization-sqlite`; only intended optimization/migration files are present.
