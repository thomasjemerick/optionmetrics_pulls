# 05_pull_security_prices.py
import wrds
import pandas as pd
from tqdm import tqdm
from config import CFG

TABLE = "secprd"

COLUMNS = [
    "secid", "date",
    "open", "high", "low", "close",
    "volume", "return",
    "cfadj", "shrout", "cfret",
    "ticker", "cusip", "sic", "index_flag", "exchange_d", "class", "issue_type", "industry_group",
]

def pull_date_chunks(db, sql_template: str, start: str, end: str, chunk_months: int = 6):
    dates = pd.date_range(start=start, end=end, freq=f"{chunk_months}MS").to_pydatetime().tolist()
    if pd.Timestamp(dates[-1]) < pd.Timestamp(end):
        dates.append(pd.Timestamp(end).to_pydatetime())

    out = []
    for i in tqdm(range(len(dates) - 1), desc="Pulling chunks"):
        a = pd.Timestamp(dates[i]).strftime("%Y-%m-%d")
        b = pd.Timestamp(dates[i + 1]).strftime("%Y-%m-%d")
        df = db.raw_sql(sql_template.format(a=a, b=b))
        out.append(df)

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

def main():
    db = wrds.Connection(wrds_username=CFG.wrds_username or None)

    desc = db.describe_table(library="optionm", table=TABLE)
    available = set(desc["name"].str.lower())
    cols = [c for c in COLUMNS if c.lower() in available]
    col_sql = ", ".join(cols)

    sql_template = f"""
        SELECT {col_sql}
        FROM optionm.{TABLE}
        WHERE date >= '{{a}}' AND date < '{{b}}'
    """

    df = pull_date_chunks(db, sql_template, CFG.start_date, CFG.end_date, chunk_months=6)
    out_path = CFG.data_dir / "optionm_security_prices.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path} | rows={len(df):,}")

    db.close()

if __name__ == "__main__":
    main()
