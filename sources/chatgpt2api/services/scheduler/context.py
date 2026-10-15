from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from threading import Event as ThreadEvent
from typing import Callable

from services.scheduler.models import AccountRequirement, Lease, Outcome


@dataclass
class ExecutionContext:
    lease: Lease | None
    requirement: AccountRequirement
    cancel_event: ThreadEvent
    replace_lease: Callable[["ExecutionContext", set[str], Outcome], Lease | None] | None = None
    lease_released: bool = False


_active_context: ContextVar[ExecutionContext | None] = ContextVar(
    "scheduler_execution_context",
    default=None,
)


def current_execution_context() -> ExecutionContext | None:
    return _active_context.get()


def install_execution_context(context: ExecutionContext):
    return _active_context.set(context)


def reset_execution_context(token) -> None:
    _active_context.reset(token)
