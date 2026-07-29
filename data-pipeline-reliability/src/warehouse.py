"""A real SQLite-backed warehouse: the target `fact_diamond_sales` table
that the pipeline's load step writes into.

This is intentionally a real database file on disk, not an in-memory
counter. The whole point of this project is to measure duplicate writes as
literal duplicate rows a stranger can open with `sqlite3` and count
themselves, rather than trusting a benchmark script's self-reported tally.

Every physical INSERT (every time `insert_batch` actually executes, as
opposed to being short-circuited by `@idempotent`) is tagged with a fresh
`load_id` (a uuid4 minted at call time). Counting `COUNT(DISTINCT load_id)`
per `batch_id` after a run is a ground-truth measure of "how many times was
this batch physically written" that does not depend on any bookkeeping done
by the benchmark or the `latch` primitives themselves -- which is exactly
what `checker/verify_results.py` uses to independently confirm the results.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Real writers to the same sqlite file must be serialized -- this lock
# exists purely so concurrent Python threads (the naive agent's abandoned
# background retries) don't collide on the file lock and raise "database is
# locked"; it has nothing to do with idempotency and does not prevent
# duplicate *logical* writes, only prevents them from corrupting each other.
_WRITE_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_diamond_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    load_id TEXT NOT NULL,
    carat REAL, cut TEXT, color TEXT, clarity TEXT,
    depth REAL, table_pct REAL, price INTEGER, x REAL, y REAL, z REAL,
    inserted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_diamond_sales_batch ON fact_diamond_sales(batch_id);
CREATE INDEX IF NOT EXISTS idx_fact_diamond_sales_load ON fact_diamond_sales(load_id);
"""

_COLUMNS = ["row_id", "carat", "cut", "color", "clarity", "depth", "table_pct", "price", "x", "y", "z"]


def init_warehouse(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def insert_batch(db_path: str, batch_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Physically insert `rows` for `batch_id`, tagged with a fresh
    `load_id`. This function has NO deduplication logic of its own -- it
    inserts every time it is called, unconditionally, exactly like a real
    `INSERT INTO fact_sales ...` warehouse call. Whether it gets called once
    or twice for the same logical batch is entirely up to the caller (the
    naive vs. protected agent loops in agent_runner.py).
    """
    load_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc).isoformat()
    with _WRITE_LOCK:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            conn.executemany(
                f"""INSERT INTO fact_diamond_sales
                    (row_id, batch_id, load_id, {", ".join(_COLUMNS[1:])}, inserted_at)
                    VALUES ({", ".join(["?"] * (len(_COLUMNS) + 3))})""",
                [
                    (r["row_id"], batch_id, load_id, *[r[c] for c in _COLUMNS[1:]], now)
                    for r in rows
                ],
            )
            conn.commit()
        finally:
            conn.close()
    return {"batch_id": batch_id, "load_id": load_id, "rows_loaded": len(rows), "status": "success"}


def total_row_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM fact_diamond_sales")
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def loads_per_batch(db_path: str) -> Dict[str, int]:
    """Ground-truth physical-insert count per batch_id, straight from SQL."""
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute(
            "SELECT batch_id, COUNT(DISTINCT load_id) FROM fact_diamond_sales GROUP BY batch_id"
        )
        return {row[0]: int(row[1]) for row in cur.fetchall()}
    finally:
        conn.close()


def rows_per_batch(db_path: str) -> Dict[str, int]:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute("SELECT batch_id, COUNT(*) FROM fact_diamond_sales GROUP BY batch_id")
        return {row[0]: int(row[1]) for row in cur.fetchall()}
    finally:
        conn.close()


def distinct_batch_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute("SELECT COUNT(DISTINCT batch_id) FROM fact_diamond_sales")
        return int(cur.fetchone()[0])
    finally:
        conn.close()
