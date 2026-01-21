# 01_discover_optionm_tables.py
import wrds
from config import CFG

KEYWORDS = [
    "opprcd",   # option prices (you already saw this)
    "secprd",   # security prices (you saw this)
    "vol", "surf", "surface",     # volatility surface (table name varies)
    "std", "standard", "atm",     # standardized options (table name varies)
    "secur", "issue", "names",    # security master tables
]

def main():
    db = wrds.Connection(wrds_username=CFG.wrds_username or None)

    # list tables in optionm library
    tables = db.list_tables(library="optionm")
    tables_sorted = sorted(tables)

    print(f"Found {len(tables_sorted)} tables in library=optionm\n")

    # print candidate matches
    print("Likely candidates:")
    for t in tables_sorted:
        low = t.lower()
        if any(k in low for k in KEYWORDS):
            print("  -", t)

    # also show descriptions if available
    print("\nIf you want column lists for a table, use db.describe_table(library='optionm', table='TABLE')")

    db.close()

if __name__ == "__main__":
    main()
