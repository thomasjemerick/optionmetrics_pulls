# wrds_pull.py
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import wrds


def pull_sql_chunked(*, sql: str, out_path: str, run_name: str) -> None:
    """
    Execute a SQL query against WRDS and write the result to a parquet file.

    - Keeps output as parquet (fast + compressed)
    - Forces date columns to real datetime when possible
    """
    out_path = str(out_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    print(f"[RUN] {run_name}")
    t0 = time.time()

    db = wrds.Connection()
    try:
        df = db.raw_sql(sql)
    finally:
        db.close()

    # Normalize any common date-like columns
    for col in ("date", "exdate", "last_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df.to_parquet(out_path, index=False)

    dt = time.time() - t0
    print(f"[OK] {run_name} rows={len(df):,} secs={dt:.1f} out={out_path}")
