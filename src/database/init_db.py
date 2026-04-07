import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database.db import get_connection


SCHEMA_PATH = Path(__file__).with_name("price_total.sql")


def _read_schema_sql() -> str:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _column_exists(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return any(column[1] == column_name for column in columns)


def init_db():
    temp_conn = get_connection()
    try:
        db_path = Path(temp_conn.execute("PRAGMA database_list;").fetchone()[2])
    finally:
        temp_conn.close()

    db_path.parent.mkdir(parents=True, exist_ok=True)

    db_file_exists = db_path.exists()
    if not db_file_exists:
        db_path.touch()

    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='price_total'"
        )
        table_exists = cursor.fetchone() is not None

        if not db_file_exists or not table_exists:
            schema_sql = _read_schema_sql()
            cursor.executescript(schema_sql)
        else:
            if not _column_exists(cursor, "price_total", "gid"):
                cursor.execute("ALTER TABLE price_total ADD COLUMN gid TEXT")

            if not _column_exists(cursor, "price_total", "fetched_at"):
                cursor.execute("ALTER TABLE price_total ADD COLUMN fetched_at TEXT")

            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_price_total_gid ON price_total(gid)"
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    print(init_db())