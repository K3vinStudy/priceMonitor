from pathlib import Path
import sqlite3

import config

DATA_DIR = config._ENV_CACHE["DATA_DIR"]
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "app.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

# 简单版
# def get_connection():
#     return sqlite3.connect(DB_PATH)