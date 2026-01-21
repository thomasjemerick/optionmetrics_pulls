# 00_test_connection.py
import wrds
from config import CFG

def main():
    db = wrds.Connection(wrds_username=CFG.wrds_username or None)

    # quick sanity query
    df = db.raw_sql("SELECT 1 AS ok;")
    print(df)

    db.close()
    print("WRDS connection OK.")

if __name__ == "__main__":
    main()
