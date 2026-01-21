# 04_pull_option_prices.py
import wrds
import pandas as pd
from tqdm import tqdm
from config import CFG

TABLE = "opprcd"

COLUMNS = [
    "secid", "date", "exdate", "cp_flag", "strike_price",
    "best_bid", "best_offer", "volume", "open_interest",
    "impl_volatility", "delta", "gamma", "vega", "theta",
    "optionid", "contract_size", "ss_flag"
]

def pull_date_chunks(db, sql_template: str, start: str, end: str, chunk_months: int = 1):
    dates = pd.date_range(start=start, end=end, freq=f"{chunk_months}MS").to_pydatetime().tolist()
    if pd.Timestamp(dates[-1]) < pd.Timestamp(end):
        dates.append(pd.Timestamp(end).to_pydatetime())

    out_paths = []
    for i in tqdm(range(len(dates) - 1), desc="Pulling monthly chunks"):
        a = pd.Timestamp(dates[i]).strftime("%Y-%m-%d")
        b = pd.Timestamp(dates[i + 1]).strftime("%Y-%m-%d")
        df = db.raw_sql(sql_template.format(a=a, b=b))

        out_path = CFG.data_dir / f"optionm_opprcd_{a}_to_{b}.parquet"
        df.to_parquet(out_path, index=False)
        out_paths.append(out_path)

    return out_paths

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

    paths = pull_date_chunks(db, sql_template, CFG.start_date, CFG.end_date, chunk_months=1)
    print(f"Saved {len(paths)} parquet chunks for opprcd.")

    db.close()

if __name__ == "__main__":
    main()
