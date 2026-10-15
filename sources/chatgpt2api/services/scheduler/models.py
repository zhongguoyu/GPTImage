from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping


class Lane(str, Enum):
    TEXT = "text"
    IMAGE_GENERATION = "image_generation"
    IMAGE_EDIT = "image_edit"


class Outcome(str, Enum):
    SUCCESS = "success"
    RATE_LIMIT = "rate_limit"
    AUTH_ERROR = "auth_error"
    NETWORK_ERROR = "network_error"
    SERVER_ERROR = "server_error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class ErrorClass(str, Enum):
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    NETWORK = "network"
    TIMEOUT = "timeout"
    SERVER = "server"


@dataclass(frozen=True)
class LaneSettings:
    worker_count: int
    global_concurrency: int
    per_account_concurrency: int
    queue_max_items: int
    queue_max_cost: int
    queue_wait_timeout_ms: int
    request_timeout_ms: int


@dataclass(frozen=True)
class SchedulerSettings:
    enabled: bool
    system_concurrency: int
    lanes: Mapping[Lane, LaneSettings]
    image_total_per_account_concurrency: int
    account_reconcile_interval_ms: int
    lease_watchdog_interval_ms: int
    snapshot_flush_interval_ms: int


@dataclass(frozen=True)
class AccountRequirement:
    lane: Lane
    model: str
    plan_type: str | None = None
    source_type: str | None = None
    cost: int = 1
    account_ref: str | None = None


@dataclass(frozen=True)
class Lease:
    lease_id: str
    access_token: str
    account_key: str
    lane: Lane
    cost: int
    deadline: float
    token_generation: int


@dataclass(frozen=True)
class SchedulerRequest:
    lane: Lane
    cost: int
    model: str
    priority: int = 0
