from __future__ import annotations


class SchedulerError(RuntimeError):
    status_code = 503

    def __init__(self, message: str, *, retry_after_seconds: float | None = None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class QueueFullError(SchedulerError):
    status_code = 429


class QueueTimeoutError(SchedulerError):
    status_code = 429


class NoProviderAvailableError(SchedulerError):
    status_code = 503


class SchedulerDeadlineError(SchedulerError):
    status_code = 504
