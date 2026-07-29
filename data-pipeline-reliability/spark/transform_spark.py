#!/usr/bin/env python3
"""PySpark version of the transform step, run as an independent
cross-check against the pandas transform (src/transform.py).

This job reads the same raw CSV extracted by src/extract.py, computes
aggregate statistics with real Spark (local mode), and writes them to
`data/spark_aggregates.json`. The row-count and price-sum it computes are
compared against the pandas-computed numbers by
`checker/verify_results.py` -- if pandas and Spark, two entirely
independent computation engines, don't agree on totals derived from the
same source file, that's a real bug worth catching, not a rounding
footnote.

Run directly (requires `pyspark` and a JRE):
    python spark/transform_spark.py data/diamonds_raw.csv data/spark_aggregates.json
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

# Some sandboxed / containerized hosts have a hostname that doesn't resolve
# via DNS or /etc/hosts (Spark's JVM calls InetAddress.getLocalHost() during
# startup and aborts with UnknownHostException otherwise). These env vars
# are real, documented Spark overrides -- setting them here means this
# script runs standalone in such environments without requiring root to
# edit /etc/hosts. On a normal machine where the hostname already resolves,
# they're harmless no-ops.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
if "HOSTALIASES" not in os.environ:
    _hostaliases_path = os.path.join(tempfile.gettempdir(), "latch_spark_hostaliases")
    with open(_hostaliases_path, "w") as _f:
        _f.write(f"{os.uname().nodename} 127.0.0.1\n")
    os.environ["HOSTALIASES"] = _hostaliases_path


def run(csv_path: str, output_path: str) -> dict:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("latch-diamond-sales-transform")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

        total_rows = df.count()
        price_sum = df.agg(F.sum("price")).collect()[0][0]
        price_min = df.agg(F.min("price")).collect()[0][0]
        price_max = df.agg(F.max("price")).collect()[0][0]

        by_cut = (
            df.groupBy("cut")
            .agg(F.count("*").alias("count"), F.avg("price").alias("avg_price"))
            .orderBy("cut")
            .collect()
        )
        by_cut_result = [
            {"cut": r["cut"], "count": int(r["count"]), "avg_price": float(r["avg_price"])}
            for r in by_cut
        ]

        by_color = (
            df.groupBy("color")
            .agg(F.count("*").alias("count"), F.avg("price").alias("avg_price"))
            .orderBy("color")
            .collect()
        )
        by_color_result = [
            {"color": r["color"], "count": int(r["count"]), "avg_price": float(r["avg_price"])}
            for r in by_color
        ]

        result = {
            "engine": "pyspark",
            "spark_version": spark.version,
            "source_csv": csv_path,
            "total_rows": int(total_rows),
            "price_sum": float(price_sum),
            "price_min": float(price_min),
            "price_max": float(price_max),
            "by_cut": by_cut_result,
            "by_color": by_color_result,
        }
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)
        return result
    finally:
        spark.stop()


def main() -> int:
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/diamonds_raw.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "data/spark_aggregates.json"
    result = run(csv_path, output_path)
    print(f"Spark {result['spark_version']} read {result['total_rows']} rows from {csv_path}")
    print(f"price_sum={result['price_sum']}  price_min={result['price_min']}  price_max={result['price_max']}")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
