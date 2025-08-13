#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, logging
from dotenv import load_dotenv
import snowflake.connector

SQL_FILE = os.path.join("sql", "transform.sql")

def need(*keys):
    """Fetch required env vars or raise clear error."""
    vals = []
    for k in keys:
        v = os.environ.get(k)
        if v is None or v == "":
            raise RuntimeError(f"Missing env var: {k}. Make sure it exists in your .env")
        vals.append(v)
    return vals if len(vals) > 1 else vals[0]

def get_conn():
    load_dotenv()  # 关键：加载项目根目录下的 .env
    user, pwd, account = need("SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT")
    role = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    wh   = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    return snowflake.connector.connect(
        user=user, password=pwd, account=account, role=role, warehouse=wh
    )

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not os.path.exists(SQL_FILE):
        logging.error("Not found: %s", SQL_FILE); sys.exit(2)

    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_text = f.read()

    # 简单按分号拆分执行；忽略空语句
    stmts = [s.strip() for s in sql_text.split(";") if s.strip()]

    conn = get_conn()
    cur = conn.cursor()
    try:
        for i, stmt in enumerate(stmts, 1):
            cur.execute(stmt)
            logging.info("Executed statement %d OK", i)
        logging.info("Transform completed.")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Transform failed:")
        sys.exit(1)

