"""A second, independent real SQLite warehouse -- `fact_housing_sales` plus
a `control_batch_status` control table -- used by `src/saga_demo.py` to
demonstrate `Saga` compensation against genuine multi-step, multi-table
writes and a genuine rollback (a real `DELETE`, not a simulated one).

Source data: the public `Housing` dataset (Windsor house sales prices --
see src/extract.py:extract_housing), a different real dataset from
`diamonds`, so the Saga demo isn't just re-using the same table.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

_WRITE_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_housing_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    price REAL, lotsize REAL, bedrooms REAL, bathrms REAL, stories REAL,
    driveway TEXT, recroom TEXT, fullbase TEXT, gashw TEXT, airco TEXT,
    garagepl REAL, prefarea TEXT,
    inserted_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS control_batch_status (
    batch_id TEXT PRIMARY KEY,
    processed INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_housing_sales_batch ON fact_housing_sales(batch_id);
"""

_COLUMNS = [
    "row_id", "price", "lotsize", "bedrooms", "bathrms", "stories",
    "driveway", "recroom", "fullbase", "gashw", "airco", "garagepl", "prefarea",
]


def init_warehouse(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def insert_housing_batch(db_path: str, batch_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _WRITE_LOCK:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            conn.executemany(
                f"""INSERT INTO fact_housing_sales
                    (batch_id, {", ".join(_COLUMNS)}, inserted_at)
                    VALUES ({", ".join(["?"] * (len(_COLUMNS) + 2))})""",
                [(batch_id, *[r[c] for c in _COLUMNS], now) for r in rows],
            )
            conn.commit()
        finally:
            conn.close()
    return {"batch_id": batch_id, "rows_loaded": len(rows), "status": "success"}


def delete_housing_batch(db_path: str, batch_id: str) -> int:
    """Compensation for insert_housing_batch: really deletes the rows."""
    with _WRITE_LOCK:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            cur = conn.execute("DELETE FROM fact_housing_sales WHERE batch_id = ?", (batch_id,))
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


def mark_processed(db_path: str, batch_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _WRITE_LOCK:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            conn.execute(
                "INSERT INTO control_batch_status (batch_id, processed, updated_at) VALUES (?, 1, ?) "
                "ON CONFLICT(batch_id) DO UPDATE SET processed=1, updated_at=excluded.updated_at",
                (batch_id, now),
            )
            conn.commit()
        finally:
            conn.close()


def mark_unprocessed(db_path: str, batch_id: str) -> None:
    """Compensation for mark_processed: really flips the control row back."""
    now = datetime.now(timezone.utc).isoformat()
    with _WRITE_LOCK:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            conn.execute(
                "INSERT INTO control_batch_status (batch_id, processed, updated_at) VALUES (?, 0, ?) "
                "ON CONFLICT(batch_id) DO UPDATE SET processed=0, updated_at=excluded.updated_at",
                (batch_id, now),
            )
            conn.commit()
        finally:
            conn.close()


def rows_for_batch(db_path: str, batch_id: str) -> int:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM fact_housing_sales WHERE batch_id = ?", (batch_id,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def is_processed(db_path: str, batch_id: str) -> bool:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute("SELECT processed FROM control_batch_status WHERE batch_id = ?", (batch_id,))
        row = cur.fetchone()
        return bool(row and row[0] == 1)
    finally:
        conn.close()


def total_row_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM fact_housing_sales")
        return int(cur.fetchone()[0])
    finally:
        conn.close()
