# inspect_parquet.py
from __future__ import annotations

import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="Path to a parquet file")
    ap.add_argument("--n", type=int, default=10, help="Rows to display")
    args = ap.parse_args()

    df = pd.read_parquet(args.path)

    print("\n=== HEAD ===")
    print(df.head(args.n))

    print("\n=== SHAPE ===")
    print(df.shape)

    print("\n=== COLUMNS ===")
    print(list(df.columns))

    print("\n=== DTYPES ===")
    print(df.dtypes)

    print("\n=== NUNIQUE (key cols if present) ===")
    key_cols = [c for c in ["secid", "date", "days", "delta", "cp_flag"] if c in df.columns]
    if key_cols:
        print(df[key_cols].nunique())
    else:
        print("No standard key columns found.")

if __name__ == "__main__":
    main()
