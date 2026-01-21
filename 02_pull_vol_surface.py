# 02_pull_vol_surface.py
import time
from datetime import date
import wrds
from tqdm import tqdm

from config import CFG
from grid import DAYS_GRID, DELTA_GRID, CP_FLAGS

def quarter_chunks(year: int):
    # returns (start_date, end_date_exclusive)
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
    # OptionMetrics volatility surface table is partitioned by year: vsurfdYYYY
    table = f"vsurfd{year}"

    secid_sql = fmt_in_list(secids, is_str=False)
    days_sql  = fmt_in_list(DAYS_GRID, is_str=False)
    delta_sql = fmt_in_list(DELTA_GRID, is_str=False)
    cp_sql    = fmt_in_list(CP_FLAGS, is_str=True)

    tag = f"optionm_vsurfd_{year}_{d0.strftime('%Y%m%d')}_{(d1 - date.resolution).strftime('%Y%m%d')}"
    # (d1 is exclusive; filename wants inclusive-ish, but it’s just a tag)
    # safer tag: use d1-1 day label without doing calendar math:
    tag = f"optionm_vsurfd_{year}_{d0.strftime('%Y%m%d')}_{(d1).strftime('%Y%m%d')}"

    sql = f"""
        SELECT
            secid,
            date,
            days,
            cp_flag,
            delta,
            impl_volatility,
            impl_strike,
            impl_premium,
            dispersion
        FROM optionm.{table}
        WHERE date >= '{d0.isoformat()}'
          AND date <  '{d1.isoformat()}'
          AND secid IN {secid_sql}
          AND days  IN {days_sql}
          AND delta IN {delta_sql}
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

    # We’ll loop years based on config start/end
    y0 = CFG.start_date.year
    y1 = CFG.end_date.year

    for y in range(y0, y1 + 1):
        # chunk within that year, but clamp to global start/end
        for (q0, q1) in quarter_chunks(y):
            d0 = max(q0, CFG.start_date)
            d1 = min(q1, CFG.end_date.replace(year=q1.year, month=q1.month, day=q1.day) if False else q1)

            # If chunk is outside global window, skip
            if d0 >= d1:
                continue

            print(f"[RUN] optionm_vsurfd_{y}_{d0.strftime('%Y%m%d')}_{d1.strftime('%Y%m%d')}")
            db = wrds.Connection()
            try:
                tag, n, secs, out = pull_one(db, y, d0, d1, secids)
            finally:
                db.close()
            print(f"[OK] {tag} rows={n:,} secs={secs:.1f} out={out}")

if __name__ == "__main__":
    main()
