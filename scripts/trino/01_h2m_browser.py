"""Trino human-to-machine: native browser OAuth2 (no token plumbing).

trino-python-client opens a browser via WebBrowserRedirectHandler. Log in
as peter or anna and the same authz story plays out:
  - peter → reads both finance.product and finance.revenue
  - anna  → reads finance.product, fails on finance.revenue
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from trino.auth import (
    CompositeRedirectHandler,
    OAuth2Authentication,
    WebBrowserRedirectHandler,
)
from trino.dbapi import connect

from lib.config import (
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
)


TRINO_HOST = "trino.localhost"
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
    print(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    print("A browser window will open for Keycloak login.")
    print("Login as: peter / iceberg   (or anna / iceberg)\n")

    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        auth=OAuth2Authentication(CompositeRedirectHandler([WebBrowserRedirectHandler()])),
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

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
