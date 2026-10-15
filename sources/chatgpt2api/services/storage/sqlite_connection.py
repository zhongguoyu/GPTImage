from __future__ import annotations

import logging
import sqlite3
import threading
import weakref
from collections.abc import Callable


logger = logging.getLogger(__name__)


class SQLiteConnectionRegistry:
    """Own one SQLite connection per live thread and reclaim dead-thread connections."""

    def __init__(
        self,
        connect: Callable[[], sqlite3.Connection],
        *,
        name: str,
    ) -> None:
        self._connect = connect
        self._name = str(name)
        self._local = threading.local()
        self._lock = threading.RLock()
        self._connections: dict[
            sqlite3.Connection,
            weakref.ReferenceType[threading.Thread],
        ] = {}

    def connection(self) -> sqlite3.Connection:
        existing = getattr(self._local, "connection", None)
        if existing is not None:
            return existing
        with self._lock:
            existing = getattr(self._local, "connection", None)
            if existing is not None:
                return existing
            self._reap_dead_threads_locked()
            connection = self._connect()
            self._connections[connection] = weakref.ref(threading.current_thread())
            self._local.connection = connection
            return connection

    def close_current_thread(self) -> None:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            return
        with self._lock:
            self._local.connection = None
            self._connections.pop(connection, None)
            self._close(connection)

    def reap_dead_threads(self) -> int:
        with self._lock:
            return self._reap_dead_threads_locked()

    def connection_count(self) -> int:
        with self._lock:
            return len(self._connections)

    def close(self) -> None:
        with self._lock:
            connections = tuple(self._connections)
            self._connections.clear()
            self._local.connection = None
            for connection in connections:
                self._close(connection)

    def _reap_dead_threads_locked(self) -> int:
        current = threading.current_thread()
        removed = 0
        for connection, owner_ref in tuple(self._connections.items()):
            owner = owner_ref()
            if owner is current or (owner is not None and owner.is_alive()):
                continue
            self._connections.pop(connection, None)
            self._close(connection)
            removed += 1
        return removed

    def _close(self, connection: sqlite3.Connection) -> None:
        try:
            connection.close()
        except Exception:
            logger.warning(
                "sqlite_connection_close_failed",
                extra={"store": self._name},
                exc_info=True,
            )
