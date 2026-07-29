"""Extract step: pull a real, public dataset and materialize it as a local
raw CSV file, the way a "landing zone" extract step in a real pipeline
would.

Data provenance (be honest about this -- see README "Data provenance")
------------------------------------------------------------------------
This sandbox's network egress is restricted to pypi.org / files.pythonhosted.org
/ github.com (no raw.githubusercontent.com, no Socrata/data.gov/NYC-open-data
APIs). Rather than fabricate data, we pull a real, well-known, citable public
dataset that ships as static package data on PyPI: `diamonds`, the 53,940-row
diamond-pricing dataset originally distributed with the `ggplot2` R package
(Wickham et al.) and made available to Python via the `pydataset` package
(`pip install pydataset`). It is one of the most widely used public reference
tables in data-science teaching material and tooling -- genuinely public,
genuinely real, not synthetic.

We treat each row as a "diamond sale" record and load it into a
`fact_diamond_sales` warehouse table, batch by batch, exactly the way the
article's `load_sales_batch` tool loads real rows into a real fact table.
The *batch boundaries* (which rows belong to "day N") are assigned by this
pipeline (see transform.py) since the source table has no date column of its
own -- that scheduling label is synthetic, the row data is not.

Run directly:
    python -m src.extract data/diamonds_raw.csv
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def extract(output_path: str) -> pd.DataFrame:
    """Fetch the public `diamonds` dataset and write it to `output_path`.

    Returns the DataFrame that was written, with a `row_id` column added so
    every source row has a stable identifier (0..53939) usable as a
    per-record key throughout the rest of the pipeline.
    """
    from pydataset import data  # imported lazily so `pip install pydataset`

    df = data("diamonds")
    df = df.reset_index(drop=True)
    df.insert(0, "row_id", df.index.astype(int))
    # pydataset's raw column is literally named "table" (table % of the
    # diamond) which collides with the SQL concept of a table -- rename for
    # clarity in the warehouse schema.
    df = df.rename(columns={"table": "table_pct"})

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return df


def extract_housing(output_path: str) -> pd.DataFrame:
    """Fetch the public `Housing` dataset (Sales Prices of Houses in the
    City of Windsor -- Anglin & Gencay, 1996; also shipped in R's `Ecdat`/
    `AER` packages and via `pydataset`) -- 546 real house-sale records. Used
    by the Saga demo (src/saga_demo.py) as a second, independent real public
    table, distinct from `diamonds`.
    """
    from pydataset import data

    df = data("Housing")
    df = df.reset_index(drop=True)
    df.insert(0, "row_id", df.index.astype(int))

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return df


def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def describe(output_path: str) -> Dict[str, Any]:
    df = pd.read_csv(output_path)
    return {
        "source": "diamonds (ggplot2, via pydataset)",
        "path": output_path,
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "sha256": sha256_of_file(output_path),
        "price_min": float(df["price"].min()),
        "price_max": float(df["price"].max()),
        "price_sum": float(df["price"].sum()),
    }


def main() -> int:
    output_path = sys.argv[1] if len(sys.argv) > 1 else "data/diamonds_raw.csv"
    df = extract(output_path)
    meta = describe(output_path)
    print(f"Extracted {meta['row_count']} rows from the public `diamonds` dataset")
    print(f"Wrote:   {output_path}")
    print(f"SHA256:  {meta['sha256']}")
    print(f"Columns: {meta['columns']}")
    meta_path = str(Path(output_path).with_suffix(".meta.json"))
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Wrote metadata: {meta_path}")

    housing_path = str(Path(output_path).parent / "housing_raw.csv")
    housing_df = extract_housing(housing_path)
    housing_sha = sha256_of_file(housing_path)
    print(f"\nExtracted {len(housing_df)} rows from the public `Housing` (Windsor) dataset")
    print(f"Wrote:   {housing_path}")
    print(f"SHA256:  {housing_sha}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
