#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, argparse, logging
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def parse_args():
    p = argparse.ArgumentParser(description="Load a CSV into Snowflake STAGING.RAW_REVIEWS")
    p.add_argument("--csv", required=True, help="path to CSV (output of scrape.py)")
    return p.parse_args()

def get_conn():
    load_dotenv()
    try:
        return snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],            # MATTCAO2001
            password=os.environ["SNOWFLAKE_PASSWORD"],    # 你的密码
            account=os.environ["SNOWFLAKE_ACCOUNT"],      # SIZZFYK-QAC52864
            role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "REVIEWS"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "STAGING"),
        )
    except KeyError as e:
        raise RuntimeError(f"Missing env var: {e}") from e

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("loader")
    args = parse_args()

    if not os.path.exists(args.csv):
        log.error(f"CSV not found: {args.csv}"); sys.exit(2)

    df = pd.read_csv(args.csv)
    log.info(f"Read CSV rows={len(df)} from {args.csv}")

    expected = {"review_id","review_text","score","thumbs_up_count","app_id",
                "app_version","user_name","review_created_at","scraped_at"}
    missing = expected - set(df.columns)
    if missing:
        log.error(f"Missing columns in CSV: {missing}"); sys.exit(3)

    conn = get_conn()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn, df, "RAW_REVIEWS",
            database=os.environ.get("SNOWFLAKE_DATABASE","REVIEWS"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA","STAGING"),
            quote_identifiers=False
        )
        if not success:
            log.error("write_pandas returned success=False"); sys.exit(1)
        log.info(f"Loaded to STAGING.RAW_REVIEWS: chunks={nchunks}, rows={nrows}")
    finally:
        conn.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
