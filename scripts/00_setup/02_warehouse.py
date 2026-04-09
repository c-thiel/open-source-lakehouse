"""Create the demo warehouse if missing; print a diff if it already exists."""

import json

import httpx
import urllib3
from lib.auth import admin_headers
from lib.config import (
    MANAGEMENT_URL,
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    WAREHOUSE_NAME,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


DESIRED_WAREHOUSE = {
    "warehouse-name": WAREHOUSE_NAME,
    "storage-profile": {
        "type": "s3",
        "bucket": S3_BUCKET,
        "key-prefix": WAREHOUSE_NAME,
        "endpoint": S3_ENDPOINT,
        "region": S3_REGION,
        "path-style-access": True,
        "flavor": "s3-compat",
        "sts-enabled": True,
    },
    "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": S3_ACCESS_KEY,
        "aws-secret-access-key": S3_SECRET_KEY,
    },
}


def get_existing(headers: dict) -> dict | None:
    response = httpx.get(
        f"{MANAGEMENT_URL}/v1/warehouse", headers=headers, verify=False
    )
    if response.status_code == 403:
        raise SystemExit(
            "\n✗ Lakekeeper rejected the bootstrap admin client (403 Forbidden).\n"
            "\n"
            "  This usually means you bootstrapped Lakekeeper manually (e.g. via\n"
            "  the UI as a human user) instead of letting `01_bootstrap.py` do it\n"
            "  with the `lakehouse-admin` client. The lakehouse-admin service\n"
            "  principal therefore has no permissions yet.\n"
            "\n"
            "  Fix: open the Lakekeeper UI as the human admin you bootstrapped\n"
            "  with, go to Server → Permissions, and grant the principal\n"
            "  `service-account-lakehouse-admin` the `operator` privilege\n"
            "  on the server. Then re-run this script.\n"
        )
    response.raise_for_status()
    for wh in response.json().get("warehouses", []):
        if wh.get("name") == WAREHOUSE_NAME:
            return wh
    return None


def diff_storage(existing: dict, desired: dict) -> list[str]:
    diffs = []
    e = existing.get("storage-profile", {})
    d = desired["storage-profile"]
    for key in d:
        if key == "type":
            continue
        ev, dv = e.get(key), d[key]
        if isinstance(ev, str) and isinstance(dv, str):
            ev, dv = ev.rstrip("/"), dv.rstrip("/")
        if ev != dv:
            diffs.append(f"  storage-profile.{key}: {ev!r} -> {dv!r}")
    return diffs


def self_provision(headers: dict) -> None:
    """Register the bootstrap admin in Lakekeeper's user list.

    Without this, the admin service principal exists in Keycloak but
    Lakekeeper has never seen it, so it doesn't show up in the UI's user
    table. Workshop attendees expect to see *all* the principals up-front.
    POSTing /v1/user with an empty body is the documented self-provision
    call — Lakekeeper extracts the identity from the bearer token.
    """
    httpx.post(f"{MANAGEMENT_URL}/v1/user", headers=headers, json={}, verify=False)


def main():
    headers = admin_headers()

    # Self-provision early so the admin shows up in the UI even before any
    # warehouse work happens.
    self_provision(headers)

    existing = get_existing(headers)

    if existing is None:
        print(f"Creating warehouse '{WAREHOUSE_NAME}'...")
        response = httpx.post(
            f"{MANAGEMENT_URL}/v1/warehouse",
            headers=headers,
            json=DESIRED_WAREHOUSE,
            verify=False,
            timeout=30,
        )
        response.raise_for_status()
        print("✓ Created.")
        print(json.dumps(response.json(), indent=2))
        return

    print(f"Warehouse '{WAREHOUSE_NAME}' already exists.")
    diffs = diff_storage(existing, DESIRED_WAREHOUSE)
    if diffs:
        print("Differences:")
        for d in diffs:
            print(d)
    else:
        print("✓ Matches desired config.")


if __name__ == "__main__":
    main()
