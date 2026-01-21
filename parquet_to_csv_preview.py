# parquet_to_csv_preview.py
from __future__ import annotations

import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("parquet_path")
    ap.add_argument("--out", default="preview.csv")
    ap.add_argument("--n", type=int, default=2000)
    args = ap.parse_args()

    df = pd.read_parquet(args.parquet_path)
    df.head(args.n).to_csv(args.out, index=False)
    print(f"Wrote {min(args.n, len(df))} rows to {args.out}")

if __name__ == "__main__":
    main()
