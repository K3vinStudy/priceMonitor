import sqlite3
import uuid
from typing import Any

from database.db import get_connection


ALLOWED_QUERY_FIELDS = {
    "ruid",
    "series",
    "price_cny",
    "date",
    "location",
    "source_url",
    "gid",
}


def insert_price_record(
    series: str,
    price_cny: float,
    date: str,
    location: str,
    source_url: str,
    gid: str,
    evidence_where: str,
    evidence_content: str,
) -> str:
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
                evidence_where,
                evidence_content
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ruid,
                series,
                float(price_cny),
                date,
                location,
                source_url,
                gid,
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


def insert_price_records(records: list[dict[str, Any]]) -> list[str]:
    if not records:
        return []

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
                evidence_where,
                evidence_content
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def get_price_record_by_ruid(ruid: str) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM price_total WHERE ruid = ?", (ruid,))
        row = cursor.fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


def list_price_records(limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
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


def query_price_records(
    series: str | None = None,
    location: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    source_url: str | None = None,
    gid: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    conditions = []
    params = []

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

    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def delete_price_record_by_ruid(ruid: str) -> bool:
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


def delete_price_records_by_gid(gid: str) -> int:
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


def count_price_records() -> int:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM price_total")
        row = cursor.fetchone()
        return int(row[0]) if row is not None else 0
    finally:
        conn.close()
