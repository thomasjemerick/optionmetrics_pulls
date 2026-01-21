# 03_pull_standardized_options.py
import time
from datetime import date
import wrds

from config import CFG
from grid import DAYS_GRID, CP_FLAGS

def quarter_chunks(year: int):
    return [
        (date(year, 1, 1),  date(year, 4, 1)),
        (date(year, 4, 1),  date(year, 7, 1)),
        (date(year, 7, 1),  date(year, 10, 1)),
        (date(year, 10, 1), date(year + 1, 1, 1)),
    ]

def fmt_in_list(xs, is_str=False):
    if is_str:
        return "(" + ",".join([f"'{x}'" for x in xs]) + ")"
    return "(" + ",".join([str(int(x)) for x in xs]) + ")"

def out_path(tag: str):
    return CFG.data_dir / f"{tag}.parquet"

def pull_one(db, year: int, d0: date, d1: date, secids):
    # Standardized option prices table partitioned by year: stdopdYYYY
    table = f"stdopd{year}"

    secid_sql = fmt_in_list(secids, is_str=False)
    days_sql  = fmt_in_list(DAYS_GRID, is_str=False)
    cp_sql    = fmt_in_list(CP_FLAGS, is_str=True)

    tag = f"optionm_stdopd_{year}_{d0.strftime('%Y%m%d')}_{d1.strftime('%Y%m%d')}"

    sql = f"""
        SELECT
            secid,
            date,
            days,
            cp_flag,
            forward_price,
            strike_price,
            premium,
            impl_volatility,
            delta,
            gamma,
            theta,
            vega
        FROM optionm.{table}
        WHERE date >= '{d0.isoformat()}'
          AND date <  '{d1.isoformat()}'
          AND secid IN {secid_sql}
          AND days  IN {days_sql}
          AND cp_flag IN {cp_sql}
    """

    t0 = time.time()
    df = db.raw_sql(sql)
    secs = time.time() - t0

    out = out_path(tag)
    df.to_parquet(out, index=False)
    return tag, len(df), secs, str(out)

def main():
    secids = CFG.load_universe_secids()
    y0 = CFG.start_date.year
    y1 = CFG.end_date.year

    for y in range(y0, y1 + 1):
        for (q0, q1) in quarter_chunks(y):
            d0 = max(q0, CFG.start_date)
            d1 = min(q1, q1)  # keep as exclusive bound

            if d0 >= d1:
                continue

            # Also clamp d1 to global end if same year-range
            if y == y1 and d1 > CFG.end_date:
                d1 = CFG.end_date

            print(f"[RUN] optionm_stdopd_{y}_{d0.strftime('%Y%m%d')}_{d1.strftime('%Y%m%d')}")
            db = wrds.Connection()
            try:
                tag, n, secs, out = pull_one(db, y, d0, d1, secids)
            finally:
                db.close()
            print(f"[OK] {tag} rows={n:,} secs={secs:.1f} out={out}")

if __name__ == "__main__":
    main()
