"""Create the demo warehouse if missing; print a diff if it already exists."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
    response = httpx.get(f"{MANAGEMENT_URL}/v1/warehouse", headers=headers, verify=False)
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


def main():
    headers = admin_headers()
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
