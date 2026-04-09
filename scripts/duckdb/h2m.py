"""DuckDB h2m: device-code OAuth flow against Keycloak, then ATTACH with the
resulting access token.

    python duckdb/h2m.py

DuckDB's iceberg extension only natively does client_credentials, but the
`CREATE SECRET ... TYPE iceberg` syntax accepts a TOKEN field that bypasses
the OAuth2 exchange and is used directly as a Bearer token. So we run the
device-code flow ourselves with httpx and hand DuckDB the result.

Login as `peter` (full warehouse) or `anna` (only finance.product).
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import httpx
import urllib3

from lib.config import CATALOG_URL, KEYCLOAK_URL, WAREHOUSE_NAME

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


HUMAN_CLIENT_ID = "lakekeeper"
DEVICE_CODE_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/auth/device"
TOKEN_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/token"


def device_code_login() -> tuple[str, str]:
    response = httpx.post(
        DEVICE_CODE_URL,
        data={"client_id": HUMAN_CLIENT_ID, "scope": "openid lakekeeper"},
        verify=False,
    )
    response.raise_for_status()
    device = response.json()

    print("\n" + "=" * 60)
    print("Open this URL in your browser:")
    print(f"  {device['verification_uri_complete']}")
    print("\nLogin as: peter / iceberg   (or anna / iceberg)")
    print("=" * 60 + "\n")

    interval = device.get("interval", 5)
    while True:
        time.sleep(interval)
        response = httpx.post(
            TOKEN_URL,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": device["device_code"],
                "client_id": HUMAN_CLIENT_ID,
            },
            verify=False,
        )
        if response.status_code == 200:
            print("✓ Logged in")
            body = response.json()
            return body["access_token"], body.get("refresh_token", "")
        error = response.json().get("error")
        if error == "authorization_pending":
            print("  Waiting for browser login...")
            continue
        if error == "slow_down":
            interval += 5
            continue
        raise RuntimeError(f"Device flow failed: {response.json()}")


access_token, refresh_token = device_code_login()

con = duckdb.connect()
con.execute("INSTALL iceberg")
con.execute("INSTALL httpfs")
con.execute("LOAD iceberg")
con.execute("LOAD httpfs")

con.execute(
    f"""
    CREATE OR REPLACE SECRET lakekeeper_human (
      TYPE iceberg,
      TOKEN '{access_token}',
      REFRESH_TOKEN '{refresh_token}',
      OAUTH2_SERVER_URI '{TOKEN_URL}',
      CLIENT_ID '{HUMAN_CLIENT_ID}'
    )
    """
)

con.execute(
    f"""
    ATTACH '{WAREHOUSE_NAME}' AS lakekeeper (
      TYPE iceberg,
      ENDPOINT '{CATALOG_URL}',
      SECRET lakekeeper_human
    )
    """
)

print("\n--- SHOW ALL TABLES ---")
print(con.execute("SHOW ALL TABLES").fetchdf())

print("\n--- finance.product ---")
print(con.execute("SELECT * FROM lakekeeper.finance.product").fetchdf())

print("\n--- finance.revenue ---")
try:
    print(con.execute("SELECT * FROM lakekeeper.finance.revenue").fetchdf())
except Exception as e:
    print(f"  ✗ {type(e).__name__}: {e}")
