"""PyIceberg human-to-machine: device code, manual (no refresh).

Pick the human user when prompted in the browser:
  - peter / iceberg  → full warehouse modify, can read both tables
  - anna  / iceberg  → table SELECT on finance.product only; revenue fails
"""

import time

import httpx
import urllib3
from lib.config import (
    CATALOG_URL,
    KEYCLOAK_URL,
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
    WAREHOUSE_NAME,
)
from pyiceberg.catalog import Catalog, load_catalog

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


HUMAN_CLIENT_ID = "lakekeeper"
DEVICE_CODE_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/auth/device"
TOKEN_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/token"


def device_code_login() -> str:
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
            return response.json()["access_token"]
        error = response.json().get("error")
        if error == "authorization_pending":
            print("  Waiting for browser login...")
            continue
        if error == "slow_down":
            interval += 5
            continue
        raise RuntimeError(f"Device flow failed: {response.json()}")


def read_table(catalog: Catalog, fqn: str) -> None:
    print(f"\n--- {fqn} ---")
    try:
        table = catalog.load_table(fqn)
        df = table.scan().to_arrow().to_pandas()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")


def main():
    token = device_code_login()

    print("\nLoading PyIceberg catalog with the user's token...")
    catalog = load_catalog(
        "lakekeeper",
        type="rest",
        uri=CATALOG_URL,
        warehouse=WAREHOUSE_NAME,
        token=token,
    )

    print("\n--- list_namespaces ---")
    for ns in catalog.list_namespaces():
        print(f"  {ns}")

    print(f"\n--- list_tables({NAMESPACE_NAME}) ---")
    try:
        for t in catalog.list_tables(NAMESPACE_NAME):
            print(f"  {t}")
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")

    read_table(catalog, PRODUCT_TABLE_FQN)
    read_table(catalog, REVENUE_TABLE_FQN)

    print("\nDone.")


if __name__ == "__main__":
    main()
