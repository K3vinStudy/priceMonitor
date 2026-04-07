import sqlite3

import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import config

config.get_env_cache()
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

if __name__ == "__main__":
    print(get_connection())