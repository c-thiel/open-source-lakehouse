"""Create a warehouse if it does not exist. Print diff if it does."""

import json

import httpx
from auth import admin_headers
from config import (
    MANAGEMENT_URL,
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)

WAREHOUSE_NAME = "demo"

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


def get_existing_warehouse(headers: dict) -> dict | None:
    """Get an existing warehouse by name, or None."""
    response = httpx.get(
        f"{MANAGEMENT_URL}/v1/warehouse",
        headers=headers,
        verify=False,
    )
    response.raise_for_status()

    for wh in response.json().get("warehouses", []):
        if wh.get("name") == WAREHOUSE_NAME:
            return wh
    return None


def diff_warehouse(existing: dict, desired: dict) -> list[str]:
    """Compare existing warehouse with desired config, return list of diffs."""
    diffs = []
    desired_profile = desired["storage-profile"]
    existing_profile = existing.get("storage-profile", {})

    for key in desired_profile:
        if key == "type":
            continue
        existing_val = existing_profile.get(key)
        desired_val = desired_profile[key]
        # Normalize trailing slashes for URL comparison
        if isinstance(existing_val, str) and isinstance(desired_val, str):
            existing_val = existing_val.rstrip("/")
            desired_val = desired_val.rstrip("/")
        if existing_val != desired_val:
            diffs.append(f"  storage-profile.{key}: {existing_val!r} -> {desired_val!r}")

    return diffs


def main():
    headers = admin_headers()

    existing = get_existing_warehouse(headers)

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
        print(f"Warehouse '{WAREHOUSE_NAME}' created successfully.")
        print(json.dumps(response.json(), indent=2))
        return

    print(f"Warehouse '{WAREHOUSE_NAME}' already exists.")
    diffs = diff_warehouse(existing, DESIRED_WAREHOUSE)
    if diffs:
        print("Differences found:")
        for d in diffs:
            print(d)
    else:
        print("No differences — warehouse matches desired config.")


if __name__ == "__main__":
    main()
