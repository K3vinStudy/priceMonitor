import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Callable, TypeVar

from database.db import get_connection


ALLOWED_QUERY_FIELDS = {
    "ruid",
    "series",
    "price_cny",
    "date",
    "location",
    "source_url",
    "gid",
    "fetched_at",
}

RETRY_TIMES = 2
RETRY_DELAY_SECONDS = 0.2

ROOT_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT_DIR / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "database.log"

logger = logging.getLogger("database.op")
if not logger.handlers:
    logger.setLevel(logging.ERROR)
    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False

T = TypeVar("T")


def _log_error(operation: str, exc: Exception) -> None:
    logger.exception("%s 失败: %s", operation, exc)


def _run_with_retry(operation: str, func: Callable[[], T], fallback: T) -> T:
    last_error: Exception | None = None

    for attempt in range(RETRY_TIMES + 1):
        try:
            return func()
        except Exception as exc:
            last_error = exc
            if attempt < RETRY_TIMES:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                _log_error(operation, exc)

    return fallback


def insert_price_record(
    series: str,
    price_cny: float,
    date: str,
    location: str | None,
    source_url: str | None,
    gid: str | None,
    fetched_at: str | None,
    evidence_where: str | None,
    evidence_content: str | None,
) -> str | None:
    def _operation() -> str:
        ruid = uuid.uuid4().hex
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO price_total (
                    ruid,
                    series,
                    price_cny,
                    date,
                    location,
                    source_url,
                    gid,
                    fetched_at,
                    evidence_where,
                    evidence_content
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ruid,
                    series,
                    float(price_cny),
                    date,
                    location,
                    source_url,
                    gid,
                    fetched_at,
                    evidence_where,
                    evidence_content,
                ),
            )
            conn.commit()
            return ruid
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return _run_with_retry("insert_price_record", _operation, None)


def insert_price_records(records: list[dict[str, Any]]) -> list[str]:
    if not records:
        return []

    def _operation() -> list[str]:
        values = []
        generated_ruids = []
        for record in records:
            ruid = uuid.uuid4().hex
            generated_ruids.append(ruid)
            values.append(
                (
                    ruid,
                    record["series"],
                    float(record["price_cny"]),
                    record["date"],
                    record.get("location"),
                    record.get("source_url"),
                    record.get("gid"),
                    record.get("fetched_at"),
                    record.get("evidence_where"),
                    record.get("evidence_content"),
                )
            )

        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO price_total (
                    ruid,
                    series,
                    price_cny,
                    date,
                    location,
                    source_url,
                    gid,
                    fetched_at,
                    evidence_where,
                    evidence_content
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            conn.commit()
            return generated_ruids
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return _run_with_retry("insert_price_records", _operation, [])


def get_price_record_by_ruid(ruid: str) -> dict[str, Any] | None:
    if not ruid:
        return None

    def _operation() -> dict[str, Any] | None:
        conn = get_connection()
        try:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM price_total WHERE ruid = ?", (ruid,))
            row = cursor.fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    return _run_with_retry("get_price_record_by_ruid", _operation, None)


def list_price_records(limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
    limit = max(0, int(limit))
    offset = max(0, int(offset))

    def _operation() -> list[dict[str, Any]]:
        conn = get_connection()
        try:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM price_total
                ORDER BY date DESC, ruid DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    return _run_with_retry("list_price_records", _operation, [])


def query_price_records(
    ruid: str | None = None,
    series: str | None = None,
    location: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    source_url: str | None = None,
    gid: str | None = None,
    fetched_at: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    limit = max(0, int(limit))
    offset = max(0, int(offset))

    conditions = []
    params = []

    if ruid:
        conditions.append("ruid = ?")
        params.append(ruid)
    if series:
        conditions.append("series = ?")
        params.append(series)
    if location:
        conditions.append("location = ?")
        params.append(location)
    if date_from:
        conditions.append("date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("date <= ?")
        params.append(date_to)
    if source_url:
        conditions.append("source_url = ?")
        params.append(source_url)
    if gid:
        conditions.append("gid = ?")
        params.append(gid)
    if fetched_at:
        conditions.append("fetched_at = ?")
        params.append(fetched_at)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = f"""
    SELECT *
    FROM price_total
    {where_clause}
    ORDER BY date DESC, ruid DESC
    LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    def _operation() -> list[dict[str, Any]]:
        conn = get_connection()
        try:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    return _run_with_retry("query_price_records", _operation, [])


def gid_exists(gid: str) -> bool:
    if not gid:
        return False

    def _operation() -> bool:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM price_total WHERE gid = ? LIMIT 1",
                (gid,),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    return _run_with_retry("gid_exists", _operation, False)


def delete_price_record_by_ruid(ruid: str) -> bool:
    if not ruid:
        return False

    def _operation() -> bool:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM price_total WHERE ruid = ?", (ruid,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return _run_with_retry("delete_price_record_by_ruid", _operation, False)


def delete_price_records_by_gid(gid: str) -> int:
    if not gid:
        return 0

    def _operation() -> int:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM price_total WHERE gid = ?", (gid,))
            conn.commit()
            return cursor.rowcount
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return _run_with_retry("delete_price_records_by_gid", _operation, 0)


def count_price_records() -> int:
    def _operation() -> int:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM price_total")
            row = cursor.fetchone()
            return int(row[0]) if row is not None else 0
        finally:
            conn.close()

    return _run_with_retry("count_price_records", _operation, 0)


def count_price_records_by_gid(gid: str) -> int:
    if not gid:
        return 0

    def _operation() -> int:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM price_total WHERE gid = ?", (gid,))
            row = cursor.fetchone()
            return int(row[0]) if row is not None else 0
        finally:
            conn.close()

    return _run_with_retry("count_price_records_by_gid", _operation, 0)
