from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Thread

import pytest

from services.storage.sqlite_connection import SQLiteConnectionRegistry


def make_registry(tmp_path: Path) -> SQLiteConnectionRegistry:
    db_path = tmp_path / "registry.db"

    def connect() -> sqlite3.Connection:
        connection = sqlite3.connect(str(db_path), check_same_thread=False)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    return SQLiteConnectionRegistry(connect, name="test")


def test_connection_is_reused_and_current_thread_close_is_idempotent(tmp_path):
    registry = make_registry(tmp_path)
    try:
        first = registry.connection()
        assert registry.connection() is first
        assert registry.connection_count() == 1

        registry.close_current_thread()
        registry.close_current_thread()

        assert registry.connection_count() == 0
        with pytest.raises(sqlite3.ProgrammingError):
            first.execute("SELECT 1")
    finally:
        registry.close()


def test_dead_thread_connection_is_reaped_before_new_connection(tmp_path):
    registry = make_registry(tmp_path)
    worker_connections = []
    try:
        def worker() -> None:
            connection = registry.connection()
            connection.execute("SELECT 1")
            worker_connections.append(connection)

        thread = Thread(target=worker)
        thread.start()
        thread.join()

        current = registry.connection()

        assert registry.connection_count() == 1
        assert current is not worker_connections[0]
        with pytest.raises(sqlite3.ProgrammingError):
            worker_connections[0].execute("SELECT 1")
    finally:
        registry.close()


def test_repeated_short_lived_executors_do_not_accumulate_connections(tmp_path):
    registry = make_registry(tmp_path)
    try:
        for _ in range(300):
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(registry.connection).result()
            assert registry.connection_count() == 1
            registry.reap_dead_threads()
            assert registry.connection_count() == 0
    finally:
        registry.close()
