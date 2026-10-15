from __future__ import annotations

import heapq
import time
import uuid
from dataclasses import dataclass, field
from threading import RLock
from typing import Callable, Iterable

from services.scheduler.models import (
    AccountRequirement,
    ErrorClass,
    Lane,
    Lease,
    Outcome,
)
from utils.helper import anonymize_token


@dataclass
class _Breaker:
    state: str = "closed"
    cooldown_until: float = 0.0
    probe_inflight: int = 0


@dataclass
class _AccountRuntime:
    account_key: str
    raw: dict[str, object]
    account_type: str
    plan_type: str
    source_type: str
    quota: int
    lane_inflight: dict[Lane, int] = field(default_factory=dict)
    image_total_inflight: int = 0
    ewma_latency_ms: dict[Lane, float] = field(default_factory=dict)
    successes: dict[Lane, int] = field(default_factory=dict)
    failures: dict[Lane, int] = field(default_factory=dict)
    consecutive_failures: dict[Lane, int] = field(default_factory=dict)
    breakers: dict[Lane, _Breaker] = field(default_factory=dict)
    revision: int = 0

    def breaker(self, lane: Lane) -> _Breaker:
        return self.breakers.setdefault(lane, _Breaker())


class AccountPool:
    """Process-local account capability index and lease manager.

    The pool never waits for capacity. Lane schedulers decide whether to queue
    or reject while this object performs a short, lock-protected reservation.
    """

    def __init__(
        self,
        accounts: Iterable[dict[str, object]],
        settings,
        *,
        model_router: Callable[[str], object] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.settings = settings
        self._model_router = model_router or (lambda _model: set())
        self._clock = clock
        self._lock = RLock()
        self._accounts: dict[str, _AccountRuntime] = {}
        self._heaps: dict[tuple[object, ...], list[tuple[float, int, str, int]]] = {}
        self._capability_keys: set[tuple[object, ...]] = set()
        self._capability_accounts: dict[tuple[object, ...], set[str]] = {}
        self._capability_states: dict[tuple[object, ...], dict[str, int]] = {}
        self._account_capability_states: dict[tuple[tuple[object, ...], str], str] = {}
        self._account_capability_keys: dict[str, set[tuple[object, ...]]] = {}
        self._capability_cooldowns: dict[
            tuple[object, ...], list[tuple[float, str, int]]
        ] = {}
        self._breaker_cooldowns: list[tuple[float, str, str]] = []
        self._breaker_cooldown_deadlines: dict[tuple[str, Lane], float] = {}
        self._account_refs: dict[str, str] = {}
        self._account_types: set[str] = set()
        self._leases: dict[str, Lease] = {}
        self._lease_accounts: dict[str, str] = {}
        self._sequence = 0
        self._selection_probe_count = 0
        self.reconcile(accounts)

    def clock(self) -> float:
        return self._clock()

    @property
    def selection_probe_count(self) -> int:
        return self._selection_probe_count

    def reset_selection_probe_count(self) -> None:
        with self._lock:
            self._selection_probe_count = 0

    def account_keys(self) -> set[str]:
        with self._lock:
            return set(self._accounts)

    def account_count(self) -> int:
        with self._lock:
            return len(self._accounts)

    def debug_heap_entry_count(self) -> int:
        with self._lock:
            return sum(len(heap) for heap in self._heaps.values())

    def debug_stale_heap_entry_count(self) -> int:
        with self._lock:
            return sum(
                1
                for heap_key, heap in self._heaps.items()
                for _score, _sequence, account_key, revision in heap
                if not self._heap_entry_is_current_locked(heap_key, account_key, revision)
            )

    def inflight(self, account_key: str, lane: Lane) -> int:
        with self._lock:
            account = self._accounts.get(account_key)
            return int(account.lane_inflight.get(lane, 0)) if account else 0

    def reconcile(self, accounts: Iterable[dict[str, object]]) -> None:
        incoming = {
            str(item.get("access_token") or "").strip(): dict(item)
            for item in accounts
            if str(item.get("access_token") or "").strip()
        }
        with self._lock:
            previous = self._accounts
            next_accounts: dict[str, _AccountRuntime] = {}
            for account_key, raw in incoming.items():
                current = previous.get(account_key)
                runtime = self._new_runtime(account_key, raw, current)
                runtime.revision = (current.revision + 1) if current else 1
                next_accounts[account_key] = runtime
            for account_key, current in previous.items():
                if account_key in next_accounts or not self._has_active_lease(account_key):
                    continue
                current.raw = {**current.raw, "status": "禁用"}
                current.revision += 1
                next_accounts[account_key] = current
            self._accounts = next_accounts
            self._heaps.clear()
            self._capability_keys.clear()
            self._capability_accounts.clear()
            self._capability_states.clear()
            self._account_capability_states.clear()
            self._account_capability_keys.clear()
            self._capability_cooldowns.clear()
            self._breaker_cooldowns.clear()
            self._breaker_cooldown_deadlines.clear()
            self._account_types = {
                account.account_type
                for account in self._accounts.values()
                if self._base_eligible_locked(account)
            }
            self._account_refs = {
                anonymize_token(account_key): account_key
                for account_key in self._accounts
            }
            for account in self._accounts.values():
                self._publish_locked(account)

    def has_capability(self, requirement: AccountRequirement) -> bool:
        """Return whether a configured account can serve this requirement.

        Current in-flight capacity is intentionally ignored. Busy accounts are
        valid queue targets; the lane decides whether to wait or reject them.
        """
        with self._lock:
            if requirement.account_ref:
                account = self._account_for_ref_locked(requirement.account_ref)
                return account is not None and self._eligible_static_locked(account, requirement)
            keys = self._heap_keys_for_requirement(requirement)
            if requirement.lane in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
                keys = keys[:1]
            return any(
                key in self._capability_keys
                for key in keys
            )

    def provider_status(self, requirement: AccountRequirement) -> tuple[str, float | None]:
        """Classify provider availability without considering current load."""
        with self._lock:
            if requirement.account_ref:
                account = self._account_for_ref_locked(requirement.account_ref)
                if account is None or not self._eligible_static_locked(account, requirement):
                    return "missing", None
                breaker = account.breaker(requirement.lane)
                if breaker.state == "open":
                    return "cooldown", max(0.0, breaker.cooldown_until - self._clock())
                if breaker.state == "half_open" and breaker.probe_inflight > 0:
                    return "unavailable", None
                return "available", None

            cooldowns: list[float] = []
            unavailable = False
            keys = list(dict.fromkeys(self._heap_keys_for_requirement(requirement)))
            if requirement.lane in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
                keys = keys[:1]
            for key in keys:
                counts = self._capability_states.get(key, {})
                if counts.get("available", 0) > 0:
                    return "available", None
                if counts.get("cooldown", 0) > 0:
                    remaining = self._cooldown_remaining_locked(key)
                    if remaining is not None:
                        cooldowns.append(remaining)
                if counts.get("unavailable", 0) > 0:
                    unavailable = True
            if cooldowns:
                return "cooldown", min(cooldowns)
            if unavailable:
                return "unavailable", None
            return ("missing", None) if not any(key in self._capability_keys for key in keys) else ("unavailable", None)

    def restore_health(self, snapshots: Iterable[dict[str, object]]) -> None:
        """Restore breaker state using redacted, stable account references."""
        with self._lock:
            by_ref = {
                anonymize_token(account.account_key): account
                for account in self._accounts.values()
            }
            now = self._clock()
            for snapshot in snapshots:
                account = by_ref.get(str(snapshot.get("account_key") or ""))
                if account is None:
                    continue
                try:
                    lane = Lane(str(snapshot.get("lane") or ""))
                except ValueError:
                    continue
                breaker = account.breaker(lane)
                state = str(snapshot.get("breaker_state") or "closed")
                breaker.state = state if state in {"closed", "open", "half_open"} else "closed"
                wall_deadline = float(snapshot.get("cooldown_until") or 0.0)
                breaker.cooldown_until = now + max(0.0, wall_deadline - time.time())
                breaker.probe_inflight = 0
                account.ewma_latency_ms[lane] = float(snapshot.get("ewma_latency_ms") or 0.0)
                account.successes[lane] = int(snapshot.get("recent_success_count") or 0)
                account.failures[lane] = int(snapshot.get("recent_failure_count") or 0)
                if breaker.state == "open" and breaker.cooldown_until <= now:
                    breaker.state = "half_open"
                account.revision += 1
            self._heaps.clear()
            self._capability_states.clear()
            self._account_capability_states.clear()
            self._account_capability_keys.clear()
            self._capability_cooldowns.clear()
            self._breaker_cooldowns.clear()
            self._breaker_cooldown_deadlines.clear()
            for account in self._accounts.values():
                self._publish_locked(account)

    def try_acquire(
        self,
        requirement: AccountRequirement,
        *,
        deadline: float,
        excluded: set[str] | None = None,
    ) -> Lease | None:
        cost = max(1, int(requirement.cost))
        excluded = set(excluded or set())
        if not float(deadline) > self._clock():
            return None
        with self._lock:
            candidate = self._best_candidate_locked(requirement, excluded, cost)
            if candidate is None:
                return None
            account = self._accounts[candidate]
            breaker = account.breaker(requirement.lane)
            if breaker.state == "half_open":
                breaker.probe_inflight += 1
            account.lane_inflight[requirement.lane] = (
                int(account.lane_inflight.get(requirement.lane, 0)) + cost
            )
            if requirement.lane in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
                account.image_total_inflight += cost
            account.revision += 1
            self._sequence += 1
            lease = Lease(
                lease_id=uuid.uuid4().hex,
                access_token=candidate,
                account_key=candidate,
                lane=requirement.lane,
                cost=cost,
                deadline=deadline,
                token_generation=int(account.raw.get("token_generation") or 0),
            )
            self._leases[lease.lease_id] = lease
            self._lease_accounts[lease.lease_id] = candidate
            self._publish_locked(account)
            return lease

    def release(self, lease: Lease | None, *, outcome: Outcome, latency_ms: float) -> None:
        if lease is None:
            return
        with self._lock:
            if lease.lease_id not in self._leases:
                return
            self._leases.pop(lease.lease_id, None)
            account_key = self._lease_accounts.pop(lease.lease_id, lease.account_key)
            account = self._accounts.get(account_key)
            if account is None:
                return
            lane_count = int(account.lane_inflight.get(lease.lane, 0))
            account.lane_inflight[lease.lane] = max(0, lane_count - lease.cost)
            if lease.lane in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
                account.image_total_inflight = max(0, account.image_total_inflight - lease.cost)
            breaker = account.breaker(lease.lane)
            if breaker.state == "half_open":
                breaker.probe_inflight = max(0, breaker.probe_inflight - 1)
            self._record_outcome_locked(account, lease.lane, outcome, latency_ms)
            account.revision += 1
            self._publish_locked(account)

    def record_failure(self, account_key: str, lane: Lane, error_class: ErrorClass) -> None:
        with self._lock:
            account = self._accounts.get(account_key)
            if account is None:
                return
            account.consecutive_failures[lane] = int(account.consecutive_failures.get(lane, 0)) + 1
            if error_class == ErrorClass.RATE_LIMIT:
                self._open_breaker_locked(account, lane, 2.0)
            elif account.consecutive_failures[lane] >= 3:
                self._open_breaker_locked(account, lane, 5.0)
            account.revision += 1
            self._publish_locked(account)

    def record_success(self, account_key: str, lane: Lane, latency_ms: float) -> None:
        with self._lock:
            account = self._accounts.get(account_key)
            if account is None:
                return
            self._record_outcome_locked(account, lane, Outcome.SUCCESS, latency_ms)
            account.revision += 1
            self._publish_locked(account)

    def open_breaker(self, account_key: str, lane: Lane, cooldown_seconds: float = 30.0) -> None:
        with self._lock:
            account = self._accounts.get(account_key)
            if account is None:
                return
            self._open_breaker_locked(account, lane, cooldown_seconds)
            account.revision += 1
            self._publish_locked(account)

    def promote_due_cooldowns(self) -> int:
        now = self._clock()
        promoted = 0
        with self._lock:
            while self._breaker_cooldowns and self._breaker_cooldowns[0][0] <= now:
                deadline, account_key, lane_value = heapq.heappop(self._breaker_cooldowns)
                lane = Lane(lane_value)
                index_key = (account_key, lane)
                if self._breaker_cooldown_deadlines.get(index_key) != deadline:
                    continue
                account = self._accounts.get(account_key)
                if account is None:
                    self._breaker_cooldown_deadlines.pop(index_key, None)
                    continue
                breaker = account.breaker(lane)
                if breaker.state != "open" or breaker.cooldown_until != deadline:
                    self._breaker_cooldown_deadlines.pop(index_key, None)
                    continue
                self._breaker_cooldown_deadlines.pop(index_key, None)
                breaker.state = "half_open"
                breaker.probe_inflight = 0
                account.revision += 1
                self._publish_locked(account)
                promoted += 1
        return promoted

    def reclaim_expired(self, now: float | None = None) -> list[Lease]:
        current_time = self._clock() if now is None else now
        with self._lock:
            expired = [lease for lease in self._leases.values() if lease.deadline <= current_time]
        for lease in expired:
            self.release(lease, outcome=Outcome.CANCELLED, latency_ms=0)
        return expired

    def snapshot_health(self) -> list[dict[str, object]]:
        with self._lock:
            items = []
            for account in self._accounts.values():
                for lane in Lane:
                    breaker = account.breaker(lane)
                    cooldown_remaining = max(0.0, breaker.cooldown_until - self._clock())
                    items.append({
                        "account_key": anonymize_token(account.account_key),
                        "lane": lane.value,
                        "breaker_state": breaker.state,
                        "cooldown_until": time.time() + cooldown_remaining,
                        "quota_snapshot": account.quota,
                        "ewma_latency_ms": account.ewma_latency_ms.get(lane, 0.0),
                        "recent_success_count": account.successes.get(lane, 0),
                        "recent_failure_count": account.failures.get(lane, 0),
                        "last_error_class": "",
                        "inflight": account.lane_inflight.get(lane, 0),
                    })
            return items

    def _new_runtime(
        self,
        account_key: str,
        raw: dict[str, object],
        previous: _AccountRuntime | None,
    ) -> _AccountRuntime:
        runtime = _AccountRuntime(
            account_key=account_key,
            raw=raw,
            account_type=self._normalize(raw.get("type")),
            plan_type=self._normalize(raw.get("plan_type") or raw.get("type")),
            source_type=self._normalize(raw.get("source_type") or raw.get("source")),
            quota=self._quota(raw.get("quota")),
        )
        if previous is not None:
            runtime.lane_inflight = dict(previous.lane_inflight)
            runtime.image_total_inflight = previous.image_total_inflight
            runtime.ewma_latency_ms = dict(previous.ewma_latency_ms)
            runtime.successes = dict(previous.successes)
            runtime.failures = dict(previous.failures)
            runtime.consecutive_failures = dict(previous.consecutive_failures)
            runtime.breakers = {
                lane: _Breaker(item.state, item.cooldown_until, item.probe_inflight)
                for lane, item in previous.breakers.items()
            }
        return runtime

    def _best_candidate_locked(
        self,
        requirement: AccountRequirement,
        excluded: set[str],
        cost: int,
    ) -> str | None:
        best: tuple[float, str] | None = None
        for heap_key in self._heap_keys_for_requirement(requirement):
            heap = self._heaps.get(heap_key)
            if not heap:
                continue
            entry = self._valid_top_locked(heap, requirement, excluded, cost)
            if entry is None:
                continue
            score, _sequence, account_key, _revision = entry
            if best is None or (score, account_key) < best:
                best = (score, account_key)
        return best[1] if best else None

    def _valid_top_locked(
        self,
        heap: list[tuple[float, int, str, int]],
        requirement: AccountRequirement,
        excluded: set[str],
        cost: int,
    ) -> tuple[float, int, str, int] | None:
        skipped: list[tuple[float, int, str, int]] = []
        selected = None
        while heap:
            entry = heapq.heappop(heap)
            self._selection_probe_count += 1
            _score, _sequence, account_key, revision = entry
            account = self._accounts.get(account_key)
            if account is None or account.revision != revision:
                continue
            if account_key in excluded:
                skipped.append(entry)
                continue
            if not self._eligible_locked(account, requirement, cost):
                continue
            selected = entry
            break
        for entry in skipped:
            heapq.heappush(heap, entry)
        if selected is not None:
            heapq.heappush(heap, selected)
        return selected

    def _eligible_locked(self, account: _AccountRuntime, requirement: AccountRequirement, cost: int) -> bool:
        if not self._eligible_static_locked(account, requirement):
            return False
        breaker = account.breaker(requirement.lane)
        if breaker.state == "open":
            return False
        if breaker.state == "half_open" and breaker.probe_inflight > 0:
            return False
        lane_settings = self.settings.lanes[requirement.lane]
        if account.lane_inflight.get(requirement.lane, 0) + cost > lane_settings.per_account_concurrency:
            return False
        if requirement.lane in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT}:
            if account.image_total_inflight + cost > self.settings.image_total_per_account_concurrency:
                return False
        return True

    def _eligible_static_locked(self, account: _AccountRuntime, requirement: AccountRequirement) -> bool:
        status = self._normalize(account.raw.get("status"))
        if status in {"禁用", "异常", "限流", "disabled", "error", "rate_limited"}:
            return False
        if requirement.account_ref and anonymize_token(account.account_key) != requirement.account_ref:
            return False
        if requirement.lane == Lane.TEXT:
            route_types = self._route_account_types(requirement.model)
            if route_types and account.account_type not in route_types:
                return False
        if requirement.lane in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT} and account.quota <= 0:
            return False
        if requirement.plan_type and account.plan_type != self._normalize(requirement.plan_type):
            return False
        if requirement.source_type and account.source_type != self._normalize(requirement.source_type):
            return False
        return True

    def _publish_locked(self, account: _AccountRuntime) -> None:
        self._unpublish_account_locked(account.account_key)
        if not self._base_eligible_locked(account):
            return
        for lane in Lane:
            breaker = account.breaker(lane)
            index_key = (account.account_key, lane)
            if breaker.state == "open":
                deadline = float(breaker.cooldown_until)
                if self._breaker_cooldown_deadlines.get(index_key) != deadline:
                    self._breaker_cooldown_deadlines[index_key] = deadline
                    heapq.heappush(
                        self._breaker_cooldowns,
                        (deadline, account.account_key, lane.value),
                    )
            else:
                self._breaker_cooldown_deadlines.pop(index_key, None)
        for key in self._bucket_keys_for_account(account):
            if key[0] in {Lane.IMAGE_GENERATION.value, Lane.IMAGE_EDIT.value} and account.quota <= 0:
                continue
            self._account_capability_keys.setdefault(account.account_key, set()).add(key)
            self._capability_keys.add(key)
            self._capability_accounts.setdefault(key, set()).add(account.account_key)
            breaker = account.breaker(self._lane_for_bucket(key))
            state = "cooldown" if breaker.state == "open" else (
                "unavailable"
                if breaker.state == "half_open" and breaker.probe_inflight > 0
                else "available"
            )
            counts = self._capability_states.setdefault(key, {})
            counts[state] = counts.get(state, 0) + 1
            self._account_capability_states[(key, account.account_key)] = state
            if state == "cooldown":
                heapq.heappush(
                    self._capability_cooldowns.setdefault(key, []),
                    (breaker.cooldown_until, account.account_key, account.revision),
                )
            self._sequence += 1
            heap = self._heaps.setdefault(key, [])
            heapq.heappush(
                heap,
                (
                    self._score_locked(account, self._lane_for_bucket(key)),
                    self._sequence,
                    account.account_key,
                    account.revision,
                ),
            )
            self._maybe_rebuild_heap_locked(key)

    def _heap_entry_is_current_locked(
        self,
        key: tuple[object, ...],
        account_key: str,
        revision: int,
    ) -> bool:
        account = self._accounts.get(account_key)
        return bool(
            account is not None
            and account.revision == revision
            and account_key in self._capability_accounts.get(key, set())
        )

    def _maybe_rebuild_heap_locked(self, key: tuple[object, ...]) -> None:
        heap = self._heaps.get(key)
        if not heap:
            return
        live_count = len(self._capability_accounts.get(key, set()))
        stale_count = max(0, len(heap) - live_count)
        threshold = max(1, min(16, live_count))
        if stale_count < threshold:
            return
        rebuilt = [
            entry
            for entry in heap
            if self._heap_entry_is_current_locked(key, entry[2], entry[3])
        ]
        heap[:] = rebuilt
        heapq.heapify(heap)

    def _unpublish_account_locked(self, account_key: str) -> None:
        for lane in Lane:
            self._breaker_cooldown_deadlines.pop((account_key, lane), None)
        keys = self._account_capability_keys.pop(account_key, set())
        for key in keys:
            self._capability_accounts.get(key, set()).discard(account_key)
            state = self._account_capability_states.pop((key, account_key), None)
            if state is not None:
                counts = self._capability_states.get(key, {})
                counts[state] = max(0, counts.get(state, 0) - 1)
                if counts.get(state) == 0:
                    counts.pop(state, None)
            if not self._capability_accounts.get(key):
                self._capability_accounts.pop(key, None)
                self._capability_states.pop(key, None)
                self._capability_cooldowns.pop(key, None)
                self._heaps.pop(key, None)
                self._capability_keys.discard(key)
            else:
                self._maybe_rebuild_heap_locked(key)

    def _account_for_ref_locked(self, account_ref: str) -> _AccountRuntime | None:
        account_key = self._account_refs.get(str(account_ref or "").strip())
        return self._accounts.get(account_key) if account_key else None

    def _cooldown_remaining_locked(self, key: tuple[object, ...]) -> float | None:
        heap = self._capability_cooldowns.get(key)
        if not heap:
            return None
        while heap:
            deadline, account_key, revision = heap[0]
            account = self._accounts.get(account_key)
            state = self._account_capability_states.get((key, account_key))
            breaker = account.breaker(self._lane_for_bucket(key)) if account is not None else None
            if (
                account is None
                or account.revision != revision
                or state != "cooldown"
                or breaker is None
                or breaker.cooldown_until != deadline
            ):
                heapq.heappop(heap)
                continue
            return max(0.0, deadline - self._clock())
        return None

    def _base_eligible_locked(self, account: _AccountRuntime) -> bool:
        status = self._normalize(account.raw.get("status"))
        return status not in {"禁用", "异常", "限流", "disabled", "error", "rate_limited"}

    def _bucket_keys_for_account(self, account: _AccountRuntime) -> list[tuple[object, ...]]:
        keys: list[tuple[object, ...]] = [(Lane.TEXT.value, account.account_type)]
        for lane in (Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT):
            for plan_type in (account.plan_type, None):
                for source_type in (account.source_type, None):
                    keys.append((lane.value, plan_type, source_type))
        return list(dict.fromkeys(keys))

    def _heap_keys_for_requirement(self, requirement: AccountRequirement) -> list[tuple[object, ...]]:
        if requirement.lane == Lane.TEXT:
            route_types = self._route_account_types(requirement.model)
            return [(Lane.TEXT.value, item) for item in route_types]
        plan_type = self._normalize(requirement.plan_type) if requirement.plan_type else None
        source_type = self._normalize(requirement.source_type) if requirement.source_type else None
        # Account buckets already publish exact and wildcard dimensions. A
        # request with a specified dimension must use its exact bucket; falling
        # back to a wildcard bucket would make provider_status report an
        # incompatible account as available.
        return [(requirement.lane.value, plan_type, source_type)]

    def _route_account_types(self, model: str) -> set[str]:
        if not model or model == "auto":
            return set(self._account_types)
        try:
            route = self._model_router(model)
            account_types = getattr(route, "account_types", route)
            normalized = {
                self._normalize(item)
                for item in (account_types or set())
                if self._normalize(item)
            }
            if normalized or hasattr(route, "account_types"):
                return normalized
            return set(self._account_types)
        except Exception:
            return set(self._account_types)

    def _lane_for_bucket(self, key: tuple[object, ...]) -> Lane:
        return Lane.TEXT if key[0] == Lane.TEXT.value else Lane(str(key[0]))

    def _score_locked(self, account: _AccountRuntime, lane: Lane) -> float:
        lane_settings = self.settings.lanes[lane]
        lane_load = account.lane_inflight.get(lane, 0) / max(1, lane_settings.per_account_concurrency)
        total_load = account.image_total_inflight / max(
            1,
            self.settings.image_total_per_account_concurrency,
        ) if lane in {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT} else lane_load
        latency = min(1.0, account.ewma_latency_ms.get(lane, 0.0) / 10000.0)
        # A fast failure is not a healthy low-latency provider. Use observed
        # failure ratio so restored health and newly failed accounts rank alike.
        failures_count = account.failures.get(lane, 0)
        observations = failures_count + account.successes.get(lane, 0)
        failures = failures_count / max(1, observations)
        jitter = (hash(account.account_key) & 1023) / 1023.0 * 1e-6
        return 0.45 * lane_load + 0.25 * total_load + 0.05 * latency + 0.25 * failures + jitter

    def _record_outcome_locked(
        self,
        account: _AccountRuntime,
        lane: Lane,
        outcome: Outcome,
        latency_ms: float,
    ) -> None:
        if outcome == Outcome.CANCELLED:
            return
        if outcome == Outcome.SUCCESS:
            previous = account.ewma_latency_ms.get(lane, 0.0)
            account.ewma_latency_ms[lane] = float(latency_ms) if previous <= 0 else previous * 0.8 + float(latency_ms) * 0.2
            account.successes[lane] = account.successes.get(lane, 0) + 1
            account.consecutive_failures[lane] = 0
            breaker = account.breaker(lane)
            if breaker.state == "half_open":
                breaker.state = "closed"
                breaker.cooldown_until = 0.0
            return
        account.failures[lane] = account.failures.get(lane, 0) + 1
        account.consecutive_failures[lane] = account.consecutive_failures.get(lane, 0) + 1
        if outcome == Outcome.RATE_LIMIT:
            self._open_breaker_locked(account, lane, 2.0)
        elif outcome in {
            Outcome.AUTH_ERROR,
            Outcome.SERVER_ERROR,
            Outcome.NETWORK_ERROR,
            Outcome.TIMEOUT,
        } and account.consecutive_failures[lane] >= 3:
            self._open_breaker_locked(account, lane, 5.0)

    def _open_breaker_locked(self, account: _AccountRuntime, lane: Lane, cooldown_seconds: float) -> None:
        breaker = account.breaker(lane)
        breaker.state = "open"
        breaker.cooldown_until = max(breaker.cooldown_until, self._clock() + cooldown_seconds)
        breaker.probe_inflight = 0

    def _has_active_lease(self, account_key: str) -> bool:
        return any(lease.account_key == account_key for lease in self._leases.values())

    @staticmethod
    def _normalize(value: object) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _quota(value: object) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0
