"""Vended credentials demo: call Iceberg REST loadTable directly.

PyIceberg (and every other Iceberg client) hides this away, but the Iceberg
REST `loadTable` response contains a `config` map that includes per-request
*vended* S3 credentials minted by Lakekeeper. The client uses those creds to
read/write the underlying object storage — it never sees the warehouse's
long-lived S3 keys.

This script reproduces what PyIceberg does on `catalog.load_table(...)`:
fetches an m2m token from Keycloak and calls the REST endpoint by hand, then
prints the returned `config` dict so you can see the vended credentials.
"""

import json

import httpx
import urllib3
from lib.auth import get_token
from lib.config import (
    CATALOG_URL,
    NAMESPACE_NAME,
    TABLE_PRODUCT,
    WAREHOUSE_NAME,
    get_sp,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main() -> None:
    sp_id, sp_secret = get_sp()
    token = get_token(sp_id, sp_secret)
    auth = {"Authorization": f"Bearer {token}"}

    # Iceberg REST first calls /v1/config to discover the prefix (Lakekeeper
    # uses the warehouse UUID, not its name) — every subsequent request goes
    # to /v1/{prefix}/...
    cfg = httpx.get(
        f"{CATALOG_URL}/v1/config",
        params={"warehouse": WAREHOUSE_NAME},
        headers=auth,
        verify=False,
    )
    cfg.raise_for_status()
    prefix = cfg.json()["defaults"]["prefix"]

    url = (
        f"{CATALOG_URL}/v1/{prefix}/namespaces/{NAMESPACE_NAME}"
        f"/tables/{TABLE_PRODUCT}"
    )
    print(f"\nGET {url}")

    response = httpx.get(
        url,
        headers={
            **auth,
            # Tell Lakekeeper we want vended credentials in the response.
            "X-Iceberg-Access-Delegation": "vended-credentials",
        },
        verify=False,
    )
    response.raise_for_status()
    payload = response.json()

    config: dict[str, str] = payload.get("config", {})
    print("\n--- config (vended credentials) ---")
    print(json.dumps(config, indent=2))

    # --- Use the vended credentials to prove they're scoped --------------
    # Lakekeeper hands out short-lived S3 creds that only allow access under
    # the table's own prefix. Listing the table location must succeed; listing
    # the parent (warehouse root) must fail with AccessDenied.
    import s3fs

    table_location = payload["metadata"]["location"]
    parent_location = table_location.rsplit("/", 1)[0]

    fs = s3fs.S3FileSystem(
        key=config["s3.access-key-id"],
        secret=config["s3.secret-access-key"],
        token=config.get("s3.session-token"),
        client_kwargs={
            "endpoint_url": config.get("s3.endpoint"),
            "region_name": config.get("client.region", "us-east-1"),
        },
    )

    print(f"\n--- list table location: {table_location} ---")
    for entry in fs.ls(table_location):
        print(f"  {entry}")

    print(f"\n--- list parent (should fail): {parent_location} ---")
    try:
        for entry in fs.ls(parent_location):
            print(f"  {entry}")
        print("  ✗ unexpected success — vended creds were not scoped!")
    except Exception as e:
        print(f"  ✓ {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
