"""StarRocks machine-to-machine: JWT login + read finance tables.

Switch service principal via WORKSHOP_SP env var (default airflow-sp-1).
The corresponding StarRocks JWT user is `service-account-<sp-name>`.

Flow:
  1. As root (native auth), CREATE USER ... IDENTIFIED WITH authentication_jwt
     for the chosen SP. Privileges come from the public role granted in
     starrocks/01_catalog.py — Lakekeeper enforces table-level authz.
  2. Fetch a Keycloak JWT via client_credentials.
  3. Connect to the StarRocks MySQL protocol over TLS using mysql-connector-
     python's OpenID Connect plugin (it requires a token file).
  4. SELECT from finance.product and finance.revenue. With sp-2, revenue
     fails because Lakekeeper denies it.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os
import tempfile

import mysql.connector
import pymysql

from lib.auth import get_token
from lib.config import (
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
    get_sp,
)


STARROCKS_HOST = "127.0.0.1"
STARROCKS_MYSQL_PORT = 30930
CATALOG_NAME = "lakekeeper"


def bootstrap_user(sr_username: str) -> None:
    """As root: ensure the JWT user exists. Privileges come from public role."""
    print(f"Bootstrapping StarRocks JWT user '{sr_username}' (as root)...")
    conn = pymysql.connect(host=STARROCKS_HOST, port=STARROCKS_MYSQL_PORT, user="root", password="")
    cur = conn.cursor()
    cur.execute(
        f"CREATE USER IF NOT EXISTS '{sr_username}' IDENTIFIED WITH authentication_jwt"
    )
    print(f"  ✓ User '{sr_username}' (catalog access via role public)")
    conn.close()


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
    # Keycloak service-account preferred_username = "service-account-<client_id>"
    sr_username = f"service-account-{sp_id}"

    bootstrap_user(sr_username)

    print(f"\nFetching JWT for {sp_id}...")
    token = get_token(sp_id, sp_secret, scope="starrocks")

    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(token)
        token_file = f.name

    try:
        print(f"\nConnecting to StarRocks as '{sr_username}' (TLS + JWT)...")
        conn = mysql.connector.connect(
            host=STARROCKS_HOST,
            port=STARROCKS_MYSQL_PORT,
            user=sr_username,
            openid_token_file=token_file,
            auth_plugin="authentication_openid_connect_client",
            ssl_verify_cert=False,
            ssl_verify_identity=False,
            use_pure=True,
        )
        cur = conn.cursor()

        query(cur, "SHOW CATALOGS")
        cur.execute(f"SET CATALOG {CATALOG_NAME}")
        query(cur, "SHOW DATABASES")
        query(cur, f"SHOW TABLES IN {NAMESPACE_NAME}")
        query(cur, f"SELECT * FROM {PRODUCT_TABLE_FQN}")
        query(cur, f"SELECT * FROM {REVENUE_TABLE_FQN}")

        conn.close()
    finally:
        os.unlink(token_file)

    print("\nDone.")


if __name__ == "__main__":
    main()
