# utils_sql.py
from __future__ import annotations

from typing import Iterable

def sql_in_list(values: Iterable, quote_strings: bool = False) -> str:
    """
    Build a safe-ish SQL IN (...) list for trusted in-code constants.
    For strings, set quote_strings=True.
    """
    vals = list(values)
    if not vals:
        raise ValueError("sql_in_list got empty list")

    if quote_strings:
        vals = [f"'{str(v)}'" for v in vals]
    else:
        vals = [str(v) for v in vals]

    return "(" + ",".join(vals) + ")"
