from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Literal

from services.config import config
from services.storage.sqlite_connection import SQLiteConnectionRegistry
from utils.log import logger

QuotaType = Literal["text", "image"]
QUOTA_DB_PATH = config.app_db_path
DEFAULT_RESERVATION_TTL_SECONDS = 900.0
QUOTA_CACHE_SIZE_KIB = -1024


class QuotaError(Exception):
    """Base class for user-key quota errors."""


class QuotaExceededError(QuotaError):
    def __init__(self, quota_type: QuotaType, requested_units: int, remaining_units: int):
        self.quota_type = quota_type
        self.requested_units = requested_units
        self.remaining_units = remaining_units
        super().__init__(f"{quota_type} quota exhausted")


class QuotaLimitError(QuotaError):
    def __init__(self, message: str = "quota limit is below current usage"):
        super().__init__(message)
        self.code = "quota_limit_below_usage"


class QuotaKeyNotFoundError(QuotaError):
    pass


@dataclass(frozen=True)
class QuotaReservation:
    reservation_id: str
    key_id: str | None
    quota_type: QuotaType
    requested_units: int
    admin_bypass: bool = False


class QuotaSettlementError(QuotaError):
    pass


def _count_chat_image_content(value: object) -> int:
    if isinstance(value, str):
        return value.count("data:image/")
    if isinstance(value, list):
        total = 0
        for item in value:
            if isinstance(item, dict) and str(item.get("type") or "").strip() in {
                "image_url",
                "image_generation_call",
            }:
                total += 1
            else:
                total += _count_chat_image_content(item)
        return total
    if isinstance(value, dict):
        if str(value.get("type") or "").strip() in {"image_url", "image_generation_call"}:
            return 1
        return sum(_count_chat_image_content(item) for item in value.values())
    return 0


def count_image_outputs(value: object) -> int:
    """Count successful image outputs across the supported response shapes."""
    if not isinstance(value, dict):
        return 0
    data = value.get("data")
    if isinstance(data, list):
        return sum(1 for item in data if isinstance(item, dict) and item)
    response = value.get("response")
    if isinstance(response, dict):
        return count_image_outputs(response)
    output = value.get("output")
    if isinstance(output, list):
        return sum(
            1
            for item in output
            if isinstance(item, dict) and str(item.get("type") or "").strip() == "image_generation_call"
        )
    choices = value.get("choices")
    if isinstance(choices, list):
        return sum(_count_chat_image_content(choice) for choice in choices)
    if str(value.get("object") or "").strip() == "image.generation.result":
        return count_image_outputs({"data": data})
    return _count_chat_image_content(value)


class QuotaAccounting:
    def __init__(self, reservation: QuotaReservation, quota_type: QuotaType, *, service: Any = None):
        self.reservation = reservation
        self.quota_type = quota_type
        self.service = service or quota_service
        self._observed_units = 0
        self._settled = False
        self._lock = threading.Lock()

    def observe(self, item: object) -> None:
        if self.quota_type == "image":
            self._observed_units += count_image_outputs(item)

    def success_units(self, result: object | None = None) -> int:
        if self.quota_type == "text":
            return 1
        observed = count_image_outputs(result) if result is not None else self._observed_units
        return min(self.reservation.requested_units, max(0, observed))

    def commit_success(self, result: object | None = None) -> None:
        with self._lock:
            if self._settled:
                return
            units = self.success_units(result)
            if self.reservation.admin_bypass:
                self._settled = True
                return
            if units <= 0:
                self.service.release(self.reservation.reservation_id)
            elif not self.service.commit(self.reservation.reservation_id, units):
                raise QuotaSettlementError("quota reservation is no longer active")
            self._settled = True

    def release(self) -> None:
        with self._lock:
            if self._settled:
                return
            if not self.reservation.admin_bypass:
                self.service.release(self.reservation.reservation_id)
            self._settled = True


def _now() -> float:
    return time.time()


def _validate_units(value: object, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise ValueError("quota units must be an integer")
    try:
        units = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("quota units must be an integer") from exc
    if units < (0 if allow_zero else 1):
        raise ValueError("quota units must be non-negative" if allow_zero else "quota units must be positive")
    return units


def _validate_concurrency(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("concurrency limit must be a positive integer")
    try:
        concurrency = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("concurrency limit must be a positive integer") from exc
    if concurrency < 1:
        raise ValueError("concurrency limit must be a positive integer")
    return concurrency


class AuthQuotaStore:
    _QUOTA_FIELDS = {
        "text": ("text_limit", "text_used", "text_reserved"),
        "image": ("image_limit", "image_used", "image_reserved"),
    }
    _CONCURRENCY_FIELDS = {
        "text": "text_concurrency",
        "image": "image_concurrency",
    }

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection_registry = SQLiteConnectionRegistry(
            self._create_connection,
            name="quota",
        )
        self._initialize_lock = threading.RLock()
        self._write_lock = threading.RLock()
        self._initialized = False

    def _create_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=5.0,
            check_same_thread=False,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute(f"PRAGMA cache_size={QUOTA_CACHE_SIZE_KIB}")
        connection.execute("PRAGMA wal_autocheckpoint=1000")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _connection(self) -> sqlite3.Connection:
        return self._connection_registry.connection()

    @contextmanager
    def _transaction(self):
        connection = self._connection()
        with self._write_lock:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            connection = self._connection()
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS key_quotas (
                    key_id TEXT PRIMARY KEY,
                    text_limit INTEGER NOT NULL DEFAULT 0 CHECK (text_limit >= 0),
                    text_used INTEGER NOT NULL DEFAULT 0 CHECK (text_used >= 0),
                    text_reserved INTEGER NOT NULL DEFAULT 0 CHECK (text_reserved >= 0),
                    image_limit INTEGER NOT NULL DEFAULT 0 CHECK (image_limit >= 0),
                    image_used INTEGER NOT NULL DEFAULT 0 CHECK (image_used >= 0),
                    image_reserved INTEGER NOT NULL DEFAULT 0 CHECK (image_reserved >= 0),
                    text_concurrency INTEGER NOT NULL DEFAULT 4 CHECK (text_concurrency > 0),
                    image_concurrency INTEGER NOT NULL DEFAULT 1 CHECK (image_concurrency > 0),
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    deleted_at REAL
                );

                CREATE TABLE IF NOT EXISTS quota_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    key_id TEXT NOT NULL,
                    quota_type TEXT NOT NULL CHECK (quota_type IN ('text', 'image')),
                    requested_units INTEGER NOT NULL CHECK (requested_units > 0),
                    committed_units INTEGER NOT NULL DEFAULT 0 CHECK (committed_units >= 0),
                    status TEXT NOT NULL CHECK (status IN ('reserved', 'committed', 'released', 'expired')),
                    request_id TEXT,
                    endpoint TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    settled_at REAL
                );

                CREATE INDEX IF NOT EXISTS idx_quota_reservations_key_status
                    ON quota_reservations(key_id, status);
                CREATE INDEX IF NOT EXISTS idx_quota_reservations_expiry
                    ON quota_reservations(expires_at, status);
                """
            )
            existing_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(key_quotas)").fetchall()
            }
            migration_defaults = {
                "text_concurrency": self._default_concurrency("text"),
                "image_concurrency": self._default_concurrency("image"),
            }
            for column, default in migration_defaults.items():
                if column not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE key_quotas ADD COLUMN {column} INTEGER NOT NULL DEFAULT {default} CHECK ({column} > 0)"
                    )
            for column in ("text_unlimited", "image_unlimited"):
                if column not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE key_quotas ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0 CHECK ({column} IN (0, 1))"
                    )
            self._initialized = True

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            self.initialize()

    @staticmethod
    def _default_concurrency(quota_type: QuotaType) -> int:
        value = config.user_key_text_concurrency if quota_type == "text" else config.user_key_image_concurrency
        try:
            return _validate_concurrency(value)
        except ValueError:
            return 1

    def _insert_empty_quota(self, connection: sqlite3.Connection, key_id: str, timestamp: float) -> None:
        connection.execute(
            """
            INSERT INTO key_quotas (
                key_id, text_concurrency, image_concurrency, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                key_id,
                self._default_concurrency("text"),
                self._default_concurrency("image"),
                timestamp,
                timestamp,
            ),
        )

    @classmethod
    def _field_names(cls, quota_type: QuotaType) -> tuple[str, str, str]:
        try:
            return cls._QUOTA_FIELDS[quota_type]
        except KeyError as exc:
            raise ValueError(f"unsupported quota type: {quota_type}") from exc

    @classmethod
    def _quota_payload(cls, row: sqlite3.Row | dict[str, object]) -> dict[str, object]:
        get = row.get if isinstance(row, dict) else row.__getitem__
        text_limit = int(get("text_limit"))
        text_used = int(get("text_used"))
        text_reserved = int(get("text_reserved"))
        image_limit = int(get("image_limit"))
        image_used = int(get("image_used"))
        image_reserved = int(get("image_reserved"))
        text_concurrency = int(get("text_concurrency"))
        image_concurrency = int(get("image_concurrency"))
        return {
            "key_id": get("key_id"),
            "text_limit": text_limit,
            "text_used": text_used,
            "text_reserved": text_reserved,
            "text_unlimited": bool(get("text_unlimited")),
            "text_remaining": None if get("text_unlimited") else max(0, text_limit - text_used - text_reserved),
            "image_limit": image_limit,
            "image_used": image_used,
            "image_reserved": image_reserved,
            "image_unlimited": bool(get("image_unlimited")),
            "image_remaining": None if get("image_unlimited") else max(0, image_limit - image_used - image_reserved),
            "text_concurrency": text_concurrency,
            "image_concurrency": image_concurrency,
            "created_at": get("created_at"),
            "updated_at": get("updated_at"),
            "deleted_at": get("deleted_at"),
        }

    def ensure_key(self, key_id: str) -> None:
        normalized_id = str(key_id or "").strip()
        if not normalized_id:
            raise ValueError("key_id is required")
        self._ensure_initialized()
        timestamp = _now()
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO key_quotas (
                    key_id, text_concurrency, image_concurrency, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(key_id) DO NOTHING
                """,
                (
                    normalized_id,
                    self._default_concurrency("text"),
                    self._default_concurrency("image"),
                    timestamp,
                    timestamp,
                ),
            )

    def ensure_keys(self, key_ids: Iterable[str]) -> None:
        normalized_ids = list(dict.fromkeys(str(key_id or "").strip() for key_id in key_ids if str(key_id or "").strip()))
        if not normalized_ids:
            return
        self._ensure_initialized()
        timestamp = _now()
        with self._transaction() as connection:
            connection.executemany(
                """
                INSERT INTO key_quotas (
                    key_id, text_concurrency, image_concurrency, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(key_id) DO NOTHING
                """,
                [
                    (
                        key_id,
                        self._default_concurrency("text"),
                        self._default_concurrency("image"),
                        timestamp,
                        timestamp,
                    )
                    for key_id in normalized_ids
                ],
            )

    def mark_deleted(self, key_id: str) -> None:
        normalized_id = str(key_id or "").strip()
        if not normalized_id:
            return
        self._ensure_initialized()
        with self._transaction() as connection:
            connection.execute(
                "UPDATE key_quotas SET deleted_at = COALESCE(deleted_at, ?), updated_at = ? WHERE key_id = ?",
                (_now(), _now(), normalized_id),
            )

    def get_quota(self, key_id: str) -> dict[str, object] | None:
        normalized_id = str(key_id or "").strip()
        if not normalized_id:
            return None
        self._ensure_initialized()
        row = self._connection().execute(
            "SELECT * FROM key_quotas WHERE key_id = ?",
            (normalized_id,),
        ).fetchone()
        return self._quota_payload(row) if row is not None else None

    def get_concurrency_limit(self, key_id: str, quota_type: QuotaType) -> int:
        normalized_id = str(key_id or "").strip()
        try:
            field = self._CONCURRENCY_FIELDS[quota_type]
        except KeyError as exc:
            raise ValueError(f"unsupported quota type: {quota_type}") from exc
        if not normalized_id:
            return self._default_concurrency(quota_type)
        self._ensure_initialized()
        row = self._connection().execute(
            f"SELECT {field}, deleted_at FROM key_quotas WHERE key_id = ?",
            (normalized_id,),
        ).fetchone()
        if row is None or row["deleted_at"] is not None:
            return self._default_concurrency(quota_type)
        try:
            return _validate_concurrency(row[field])
        except ValueError:
            return self._default_concurrency(quota_type)

    def list_reservations(self, key_id: str) -> list[dict[str, object]]:
        normalized_id = str(key_id or "").strip()
        if not normalized_id:
            return []
        self._ensure_initialized()
        rows = self._connection().execute(
            "SELECT * FROM quota_reservations WHERE key_id = ? ORDER BY created_at",
            (normalized_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_reservation(self, reservation_id: str) -> dict[str, object] | None:
        normalized_id = str(reservation_id or "").strip()
        if not normalized_id:
            return None
        self._ensure_initialized()
        row = self._connection().execute(
            "SELECT * FROM quota_reservations WHERE reservation_id = ?",
            (normalized_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def list_user_quotas(self, items: list[dict[str, object]]) -> list[dict[str, object]]:
        user_items = [item for item in items if str(item.get("role") or "").strip().lower() == "user"]
        self.ensure_keys(str(item.get("id") or "") for item in user_items)
        result: list[dict[str, object]] = []
        for item in items:
            normalized = dict(item)
            key_id = str(item.get("id") or "").strip()
            quota = self.get_quota(key_id) if key_id else None
            if quota is not None:
                normalized.update({key: value for key, value in quota.items() if key != "key_id"})
            result.append(normalized)
        return result

    def set_limits(
        self,
        key_id: str,
        *,
        text_unlimited: bool | None = None,
        image_unlimited: bool | None = None,
        text_limit: int | None = None,
        image_limit: int | None = None,
        text_concurrency: int | None = None,
        image_concurrency: int | None = None,
    ) -> dict[str, object]:
        normalized_id = str(key_id or "").strip()
        if not normalized_id:
            raise QuotaKeyNotFoundError("key_id is required")
        if (
            text_unlimited is None
            and image_unlimited is None
            and text_limit is None
            and image_limit is None
            and text_concurrency is None
            and image_concurrency is None
        ):
            current = self.get_quota(normalized_id)
            if current is None:
                raise QuotaKeyNotFoundError("quota row not found")
            return current
        normalized_text = _validate_units(text_limit, allow_zero=True) if text_limit is not None else None
        normalized_image = _validate_units(image_limit, allow_zero=True) if image_limit is not None else None
        normalized_text_concurrency = (
            _validate_concurrency(text_concurrency) if text_concurrency is not None else None
        )
        normalized_image_concurrency = (
            _validate_concurrency(image_concurrency) if image_concurrency is not None else None
        )
        self._ensure_initialized()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM key_quotas WHERE key_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None:
                timestamp = _now()
                self._insert_empty_quota(connection, normalized_id, timestamp)
                row = connection.execute(
                    "SELECT * FROM key_quotas WHERE key_id = ?",
                    (normalized_id,),
                ).fetchone()
            if row["deleted_at"] is not None:
                raise QuotaKeyNotFoundError("quota row not found")
            next_text_unlimited = bool(row["text_unlimited"]) if text_unlimited is None else bool(text_unlimited)
            next_image_unlimited = bool(row["image_unlimited"]) if image_unlimited is None else bool(image_unlimited)
            next_text = normalized_text if normalized_text is not None else int(row["text_limit"])
            next_image = normalized_image if normalized_image is not None else int(row["image_limit"])
            next_text_concurrency = (
                normalized_text_concurrency
                if normalized_text_concurrency is not None
                else int(row["text_concurrency"])
            )
            next_image_concurrency = (
                normalized_image_concurrency
                if normalized_image_concurrency is not None
                else int(row["image_concurrency"])
            )
            if not next_text_unlimited and next_text < int(row["text_used"]) + int(row["text_reserved"]):
                raise QuotaLimitError()
            if not next_image_unlimited and next_image < int(row["image_used"]) + int(row["image_reserved"]):
                raise QuotaLimitError()
            timestamp = _now()
            connection.execute(
                """
                UPDATE key_quotas
                SET text_unlimited = ?, image_unlimited = ?, text_limit = ?, image_limit = ?,
                    text_concurrency = ?, image_concurrency = ?, updated_at = ?
                WHERE key_id = ?
                """,
                (
                    int(next_text_unlimited),
                    int(next_image_unlimited),
                    next_text,
                    next_image,
                    next_text_concurrency,
                    next_image_concurrency,
                    timestamp,
                    normalized_id,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM key_quotas WHERE key_id = ?",
                (normalized_id,),
            ).fetchone()
        return self._quota_payload(updated)

    def reserve(
        self,
        key_id: str,
        quota_type: QuotaType,
        units: int,
        *,
        request_id: str = "",
        endpoint: str = "",
        expires_at: float | None = None,
    ) -> QuotaReservation:
        normalized_id = str(key_id or "").strip()
        if not normalized_id:
            raise ValueError("key_id is required")
        normalized_units = _validate_units(units)
        limit_field, used_field, reserved_field = self._field_names(quota_type)
        self._ensure_initialized()
        timestamp = _now()
        expiry = float(expires_at if expires_at is not None else timestamp + DEFAULT_RESERVATION_TTL_SECONDS)
        reservation_id = uuid.uuid4().hex
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM key_quotas WHERE key_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None:
                self._insert_empty_quota(connection, normalized_id, timestamp)
                row = connection.execute(
                    "SELECT * FROM key_quotas WHERE key_id = ?",
                    (normalized_id,),
                ).fetchone()
            if row["deleted_at"] is not None:
                raise QuotaExceededError(quota_type, normalized_units, 0)
            remaining = int(row[limit_field]) - int(row[used_field]) - int(row[reserved_field])
            if not row[f"{quota_type}_unlimited"] and remaining < normalized_units:
                raise QuotaExceededError(quota_type, normalized_units, max(0, remaining))
            connection.execute(
                f"UPDATE key_quotas SET {reserved_field} = {reserved_field} + ?, updated_at = ? WHERE key_id = ?",
                (normalized_units, timestamp, normalized_id),
            )
            connection.execute(
                """
                INSERT INTO quota_reservations (
                    reservation_id, key_id, quota_type, requested_units,
                    committed_units, status, request_id, endpoint,
                    created_at, expires_at
                ) VALUES (?, ?, ?, ?, 0, 'reserved', ?, ?, ?, ?)
                """,
                (
                    reservation_id,
                    normalized_id,
                    quota_type,
                    normalized_units,
                    str(request_id or "").strip() or None,
                    str(endpoint or "").strip(),
                    timestamp,
                    expiry,
                ),
            )
        return QuotaReservation(reservation_id, normalized_id, quota_type, normalized_units)

    def commit(self, reservation_id: str, committed_units: int) -> bool:
        normalized_id = str(reservation_id or "").strip()
        if not normalized_id:
            return False
        normalized_units = _validate_units(committed_units, allow_zero=True)
        if normalized_id.startswith("admin-"):
            return True
        self._ensure_initialized()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM quota_reservations WHERE reservation_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None or row["status"] != "reserved":
                return False
            requested_units = int(row["requested_units"])
            if normalized_units > requested_units:
                raise ValueError("committed units exceed reserved units")
            _, used_field, reserved_field = self._field_names(str(row["quota_type"]))
            timestamp = _now()
            updated = connection.execute(
                f"""
                UPDATE key_quotas
                SET {reserved_field} = {reserved_field} - ?,
                    {used_field} = {used_field} + ?,
                    updated_at = ?
                WHERE key_id = ? AND {reserved_field} >= ?
                """,
                (
                    requested_units,
                    normalized_units,
                    timestamp,
                    row["key_id"],
                    requested_units,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("quota summary is inconsistent with reservation")
            connection.execute(
                """
                UPDATE quota_reservations
                SET committed_units = ?, status = 'committed', settled_at = ?
                WHERE reservation_id = ? AND status = 'reserved'
                """,
                (normalized_units, timestamp, normalized_id),
            )
        return True

    def release(self, reservation_id: str, *, status: str = "released") -> bool:
        normalized_id = str(reservation_id or "").strip()
        if not normalized_id:
            return False
        if normalized_id.startswith("admin-"):
            return True
        if status not in {"released", "expired"}:
            raise ValueError("invalid release status")
        self._ensure_initialized()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM quota_reservations WHERE reservation_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None or row["status"] != "reserved":
                return False
            _, _, reserved_field = self._field_names(str(row["quota_type"]))
            timestamp = _now()
            updated = connection.execute(
                f"""
                UPDATE key_quotas
                SET {reserved_field} = {reserved_field} - ?, updated_at = ?
                WHERE key_id = ? AND {reserved_field} >= ?
                """,
                (int(row["requested_units"]), timestamp, row["key_id"], int(row["requested_units"])),
            )
            if updated.rowcount != 1:
                raise RuntimeError("quota summary is inconsistent with reservation")
            connection.execute(
                """
                UPDATE quota_reservations
                SET status = ?, settled_at = ?
                WHERE reservation_id = ? AND status = 'reserved'
                """,
                (status, timestamp, normalized_id),
            )
        return True

    def renew(self, reservation_id: str, *, expires_at: float | None = None) -> bool:
        normalized_id = str(reservation_id or "").strip()
        if not normalized_id:
            return False
        self._ensure_initialized()
        timestamp = _now()
        requested_expiry = float(
            expires_at if expires_at is not None else timestamp + DEFAULT_RESERVATION_TTL_SECONDS
        )
        if requested_expiry <= timestamp:
            raise ValueError("reservation expiry must be in the future")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT expires_at FROM quota_reservations WHERE reservation_id = ? AND status = 'reserved'",
                (normalized_id,),
            ).fetchone()
            if row is None or float(row["expires_at"]) <= timestamp:
                return False
            updated = connection.execute(
                """
                UPDATE quota_reservations
                SET expires_at = ?
                WHERE reservation_id = ? AND status = 'reserved' AND expires_at > ?
                """,
                (max(float(row["expires_at"]), requested_expiry), normalized_id, timestamp),
            )
        return updated.rowcount == 1

    def recover_expired(self, now: float | None = None) -> int:
        self._ensure_initialized()
        timestamp = float(now if now is not None else _now())
        recovered = 0
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT reservation_id
                FROM quota_reservations
                WHERE status = 'reserved' AND expires_at <= ?
                ORDER BY expires_at
                """,
                (timestamp,),
            ).fetchall()
            for row in rows:
                reservation_id = str(row["reservation_id"])
                reservation = connection.execute(
                    "SELECT * FROM quota_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                if reservation is None or reservation["status"] != "reserved":
                    continue
                _, _, reserved_field = self._field_names(str(reservation["quota_type"]))
                updated = connection.execute(
                    f"""
                    UPDATE key_quotas
                    SET {reserved_field} = {reserved_field} - ?, updated_at = ?
                    WHERE key_id = ? AND {reserved_field} >= ?
                    """,
                    (
                        int(reservation["requested_units"]),
                        timestamp,
                        reservation["key_id"],
                        int(reservation["requested_units"]),
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("quota summary is inconsistent with expired reservation")
                connection.execute(
                    """
                    UPDATE quota_reservations
                    SET status = 'expired', settled_at = ?
                    WHERE reservation_id = ? AND status = 'reserved'
                    """,
                    (timestamp, reservation_id),
                )
                recovered += 1
        return recovered

    def snapshot_bytes(self) -> bytes:
        self._ensure_initialized()
        source = self._connection()
        destination = sqlite3.connect(":memory:")
        try:
            source.backup(destination)
            return destination.serialize()
        finally:
            destination.close()

    def close(self) -> None:
        with self._initialize_lock:
            self._connection_registry.close()

    def close_current_thread(self) -> None:
        self._connection_registry.close_current_thread()

    def reap_dead_threads(self) -> int:
        return self._connection_registry.reap_dead_threads()

    def connection_count(self) -> int:
        return self._connection_registry.connection_count()


class QuotaService:
    def __init__(self, store: AuthQuotaStore | None = None):
        self.store = store or AuthQuotaStore(config.app_db_path)

    def initialize(self) -> None:
        self.store.initialize()

    def ensure_key(self, key_id: str) -> None:
        self.store.ensure_key(key_id)

    def ensure_user_keys(self, items: Iterable[dict[str, object]]) -> None:
        self.store.ensure_keys(
            str(item.get("id") or "")
            for item in items
            if str(item.get("role") or "").strip().lower() == "user"
        )

    def mark_deleted(self, key_id: str) -> None:
        self.store.mark_deleted(key_id)

    def reserve(
        self,
        identity: dict[str, object],
        quota_type: QuotaType,
        units: int,
        *,
        request_id: str = "",
        endpoint: str = "",
        expires_at: float | None = None,
    ) -> QuotaReservation:
        if str(identity.get("role") or "").strip().lower() == "admin":
            return QuotaReservation(
                reservation_id=f"admin-{uuid.uuid4().hex}",
                key_id=None,
                quota_type=quota_type,
                requested_units=_validate_units(units),
                admin_bypass=True,
            )
        key_id = str(identity.get("id") or "").strip()
        if not key_id:
            raise QuotaError("ordinary user identity has no key id")
        return self.store.reserve(
            key_id,
            quota_type,
            units,
            request_id=request_id,
            endpoint=endpoint,
            expires_at=expires_at,
        )

    def commit(self, reservation_id: str, committed_units: int) -> bool:
        return self.store.commit(reservation_id, committed_units)

    def release(self, reservation_id: str) -> bool:
        return self.store.release(reservation_id)

    def renew(self, reservation_id: str, *, expires_at: float | None = None) -> bool:
        if str(reservation_id or "").strip().startswith("admin-"):
            return True
        return self.store.renew(reservation_id, expires_at=expires_at)

    def expire(self, reservation_id: str) -> bool:
        return self.store.release(reservation_id, status="expired")

    def recover_expired(self, now: float | None = None) -> int:
        return self.store.recover_expired(now)

    def get_quota(self, key_id: str) -> dict[str, object] | None:
        return self.store.get_quota(key_id)

    def get_concurrency_limit(self, key_id: str, quota_type: QuotaType) -> int:
        return self.store.get_concurrency_limit(key_id, quota_type)

    def list_user_quotas(self, items: list[dict[str, object]]) -> list[dict[str, object]]:
        return self.store.list_user_quotas(items)

    def set_limits(
        self,
        key_id: str,
        *,
        text_unlimited: bool | None = None,
        image_unlimited: bool | None = None,
        text_limit: int | None = None,
        image_limit: int | None = None,
        text_concurrency: int | None = None,
        image_concurrency: int | None = None,
    ) -> dict[str, object]:
        return self.store.set_limits(
            key_id,
            text_unlimited=text_unlimited,
            image_unlimited=image_unlimited,
            text_limit=text_limit,
            image_limit=image_limit,
            text_concurrency=text_concurrency,
            image_concurrency=image_concurrency,
        )

    def get_reservation(self, reservation_id: str) -> dict[str, object] | None:
        return self.store.get_reservation(reservation_id)

    def list_reservations(self, key_id: str) -> list[dict[str, object]]:
        return self.store.list_reservations(key_id)

    def snapshot_bytes(self) -> bytes:
        return self.store.snapshot_bytes()

    def close(self) -> None:
        self.store.close()

    def start_watchdog(self, stop_event: threading.Event, interval_seconds: float = 5.0) -> threading.Thread:
        def run() -> None:
            try:
                while not stop_event.wait(interval_seconds):
                    try:
                        self.recover_expired()
                    except Exception as exc:
                        logger.error({"event": "quota_reservation_recovery_failed", "error": str(exc)})
            finally:
                self.store.close_current_thread()

        thread = threading.Thread(target=run, name="quota-reservation-watchdog", daemon=True)
        thread.start()
        return thread


quota_service = QuotaService()
