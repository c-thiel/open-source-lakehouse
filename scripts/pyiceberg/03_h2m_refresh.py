"""PyIceberg human-to-machine: device code with token refresh.

Same browser flow as 02, but plugged into a custom PyIceberg AuthManager
that tracks the refresh_token and silently refreshes before expiry. Long-
running notebooks/dashboards/ETL need this — without it, every catalog
call would start failing once the access token expires after a few minutes.

Pick the user when prompted: peter (full) or anna (product only).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import threading
import time

import httpx
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest.auth import AuthManager, AuthManagerFactory

from lib.config import (
    CATALOG_URL,
    KEYCLOAK_URL,
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
    WAREHOUSE_NAME,
)


HUMAN_CLIENT_ID = "lakekeeper"
DEVICE_CODE_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/auth/device"
TOKEN_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/token"
REFRESH_MARGIN_SECONDS = 30


def device_code_login() -> dict:
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
            return response.json()
        error = response.json().get("error")
        if error == "authorization_pending":
            print("  Waiting for browser login...")
            continue
        if error == "slow_down":
            interval += 5
            continue
        raise RuntimeError(f"Device flow failed: {response.json()}")


class DeviceCodeAuthManager(AuthManager):
    """PyIceberg AuthManager that handles device code login + token refresh.

    State lives on the class so PyIceberg can instantiate the manager
    multiple times (e.g. before and after _fetch_config) without prompting
    a fresh browser login.
    """

    _lock = threading.Lock()
    _token: str | None = None
    _refresh_token: str | None = None
    _expires_at: float = 0

    def __init__(self):
        with DeviceCodeAuthManager._lock:
            if DeviceCodeAuthManager._token is None:
                self._login()

    @classmethod
    def _login(cls) -> None:
        cls._apply(device_code_login())

    @classmethod
    def _apply(cls, token_response: dict) -> None:
        cls._token = token_response["access_token"]
        cls._refresh_token = token_response.get("refresh_token")
        cls._expires_at = (
            time.monotonic() + token_response["expires_in"] - REFRESH_MARGIN_SECONDS
        )

    @classmethod
    def _try_refresh(cls) -> bool:
        if not cls._refresh_token:
            return False
        try:
            response = httpx.post(
                TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "client_id": HUMAN_CLIENT_ID,
                    "refresh_token": cls._refresh_token,
                },
                verify=False,
            )
            if response.status_code != 200:
                return False
            cls._apply(response.json())
            print("  ↻ Refreshed access token")
            return True
        except Exception:
            return False

    def auth_header(self) -> str:
        with DeviceCodeAuthManager._lock:
            if (
                not DeviceCodeAuthManager._token
                or time.monotonic() >= DeviceCodeAuthManager._expires_at
            ):
                if not DeviceCodeAuthManager._try_refresh():
                    print("  Refresh failed — re-authenticating via device code...")
                    DeviceCodeAuthManager._login()
            return f"Bearer {DeviceCodeAuthManager._token}"


AuthManagerFactory.register("device-code", DeviceCodeAuthManager)


def read_table(catalog, fqn: str) -> None:
    print(f"\n--- {fqn} ---")
    try:
        table = catalog.load_table(fqn)
        df = table.scan().to_arrow().to_pandas()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")


def main():
    print("Loading PyIceberg catalog (custom AuthManager handles login + refresh)...")
    catalog = load_catalog(
        "lakekeeper",
        type="rest",
        uri=CATALOG_URL,
        warehouse=WAREHOUSE_NAME,
        auth={"type": "device-code"},
        **{"ssl": {"cabundle": False}},
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

    print("\nDone. The catalog will automatically refresh tokens as needed.")


if __name__ == "__main__":
    main()
