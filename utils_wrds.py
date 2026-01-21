from __future__ import annotations
import datetime as dt

def month_chunks(start: dt.date, end: dt.date, months: int = 3):
    """
    Yield (chunk_start, chunk_end) where chunk_end is exclusive.
    """
    def add_months(d: dt.date, m: int):
        y = d.year + (d.month - 1 + m) // 12
        mo = (d.month - 1 + m) % 12 + 1
        # clamp day to month length
        import calendar
        last = calendar.monthrange(y, mo)[1]
        day = min(d.day, last)
        return dt.date(y, mo, day)

    cur = start
    while cur < end:
        nxt = add_months(cur, months)
        if nxt > end:
            nxt = end
        yield (cur, nxt)
        cur = nxt

def read_tickers(path: str):
    tickers = []
    with open(path, "r") as f:
        for line in f:
            t = line.strip().upper()
            if t:
                tickers.append(t)
    return tickers

def sql_in_list(items):
    # returns ('A','B','C')
    safe = [str(x).replace("'", "''") for x in items]
    return "(" + ",".join(f"'{x}'" for x in safe) + ")"
