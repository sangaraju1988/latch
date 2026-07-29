"""Transform step: turn the flat extracted CSV into a sequence of daily
load batches, the unit an agentic orchestration layer would hand to a
`load_sales_batch`-style tool one batch at a time.

The source `diamonds` table has no date column, so batch boundaries (which
rows belong to "day N") are assigned here by simple sequential chunking --
that scheduling label is synthetic bookkeeping introduced by this pipeline,
not part of the source data. Every value inside a batch (carat, cut, color,
clarity, price, dimensions) is a real, unmodified value from the public
dataset.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import pandas as pd

DEFAULT_BATCH_SIZE = 1349  # yields exactly 40 batches out of 53,940 rows
DEFAULT_START_DATE = date(2026, 1, 1)


@dataclass(frozen=True)
class Batch:
    batch_id: str
    rows: List[Dict[str, Any]]

    def __len__(self) -> int:
        return len(self.rows)


def make_batches(
    df: pd.DataFrame,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_date: date = DEFAULT_START_DATE,
    prefix: str = "diamonds",
) -> List[Batch]:
    """Chunk `df` into sequential batches of at most `batch_size` rows,
    each labeled with a synthetic calendar date starting at `start_date`.
    `prefix` distinguishes batch_id namespaces across datasets (e.g.
    "diamonds" vs "housing") sharing this same chunking helper.
    """
    batches: List[Batch] = []
    n = len(df)
    num_batches = math.ceil(n / batch_size)
    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, n)
        chunk = df.iloc[start:end]
        batch_date = start_date + timedelta(days=i)
        batch_id = f"{prefix}-{batch_date.isoformat()}"
        batches.append(Batch(batch_id=batch_id, rows=chunk.to_dict("records")))
    return batches


def load_and_batch(
    csv_path: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_date: date = DEFAULT_START_DATE,
    prefix: str = "diamonds",
) -> Tuple[pd.DataFrame, List[Batch]]:
    df = pd.read_csv(csv_path)
    return df, make_batches(df, batch_size=batch_size, start_date=start_date, prefix=prefix)


def summarize_batches(batches: List[Batch]) -> Dict[str, Any]:
    sizes = [len(b) for b in batches]
    return {
        "num_batches": len(batches),
        "total_rows": sum(sizes),
        "min_batch_size": min(sizes) if sizes else 0,
        "max_batch_size": max(sizes) if sizes else 0,
        "batch_ids": [b.batch_id for b in batches],
    }


if __name__ == "__main__":
    import json
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/diamonds_raw.csv"
    _df, batches = load_and_batch(csv_path)
    summary = summarize_batches(batches)
    print(json.dumps(summary, indent=2))
