"""Trino machine-to-machine: fetch a JWT, pass to Trino, no refresh.

Switch service principal via WORKSHOP_SP env var:
  WORKSHOP_SP=airflow-sp-1 → reads both tables
  WORKSHOP_SP=airflow-sp-2 → reads product, fails on revenue
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from trino.auth import JWTAuthentication
from trino.dbapi import connect

from lib.auth import get_token
from lib.config import (
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
    get_sp,
)


TRINO_HOST = "trino.localtest.me"
TRINO_PORT = 30443
CATALOG = "lakekeeper"


def query(cur, sql: str) -> None:
    print(f"\n--- {sql} ---")
    try:
        cur.execute(sql)
        for row in cur.fetchall():
            print(f"  {row}")
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")


def main():
    sp_id, sp_secret = get_sp()

    print(f"Fetching token for {sp_id}...")
    token = get_token(sp_id, sp_secret, scope="trino")

    print(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        auth=JWTAuthentication(token),
        http_scheme="https",
        verify=False,
        catalog=CATALOG,
        schema=NAMESPACE_NAME,
    )
    cur = conn.cursor()

    query(cur, "SHOW CATALOGS")
    query(cur, f"SHOW SCHEMAS FROM {CATALOG}")
    query(cur, f"SHOW TABLES FROM {CATALOG}.{NAMESPACE_NAME}")
    query(cur, f"SELECT * FROM {CATALOG}.{PRODUCT_TABLE_FQN}")
    query(cur, f"SELECT * FROM {CATALOG}.{REVENUE_TABLE_FQN}")

    print("\nDone.")


if __name__ == "__main__":
    main()
