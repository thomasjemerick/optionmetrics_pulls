import sys
import pandas as pd
import wrds

from config import CFG
from utils_wrds import read_tickers, sql_in_list

CANDIDATE_TABLES = [
    "security",          # common
    "securd",            # common
    "securd1",           # sometimes
    "optionmnames",      # sometimes
    "security_name",     # sometimes
]

def table_cols(db, table):
    desc = db.describe_table(library="optionm", table=table)
    return [c.lower() for c in desc["name"].tolist()]

def find_mapping_table(db):
    for t in CANDIDATE_TABLES:
        try:
            cols = set(table_cols(db, t))
        except Exception:
            continue
        if "secid" in cols and "ticker" in cols:
            return t
    return None

def main():
    tickers = read_tickers("universe_tickers.txt")
    if not tickers:
        print("No tickers found in universe_tickers.txt")
        sys.exit(1)

    db = wrds.Connection(wrds_username=CFG.wrds_username or None)

    t = find_mapping_table(db)
    if not t:
        print("Could not find a mapping table with columns (secid, ticker).")
        print("Try listing columns with: db.describe_table('optionm','security') etc.")
        sys.exit(1)

    print(f"Using mapping table: optionm.{t}")
    tick_sql = sql_in_list(tickers)

    # Pull a mapping (distinct) — keep latest ticker mapping if duplicates exist
    sql = f"""
        SELECT DISTINCT secid, ticker
        FROM optionm.{t}
        WHERE ticker IN {tick_sql}
    """
    df = db.raw_sql(sql)
    db.close()

    if df.empty:
        print("Mapping query returned 0 rows. Your tickers may not exist in OptionMetrics.")
        sys.exit(1)

    # Clean up
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.dropna(subset=["secid", "ticker"])
    df["secid"] = df["secid"].astype(int)

    # Print a quick mapping view
    print(df.sort_values("ticker").to_string(index=False))

    secids = sorted(df["secid"].unique().tolist())
    with open("universe_secid.txt", "w") as f:
        for s in secids:
            f.write(str(s) + "\n")

    missing = sorted(set(tickers) - set(df["ticker"].unique()))
    if missing:
        print("\nTickers not found in mapping table:", missing)

    print(f"\nWrote {len(secids)} unique secids to universe_secid.txt")

if __name__ == "__main__":
    main()
