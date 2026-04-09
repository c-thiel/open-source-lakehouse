"""Create finance.product and finance.revenue and load workshop seed data.

Run as the bootstrap admin (lakehouse-admin) — at this point in the setup
sequence the per-warehouse permissions don't exist yet, and the admin has
all rights anyway. The same script is idempotent: if the namespace/tables
already exist with the right schema, it appends nothing.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

from lib.config import (
    ADMIN_CLIENT_ID,
    ADMIN_CLIENT_SECRET,
    CATALOG_URL,
    KEYCLOAK_TOKEN_URL,
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    WAREHOUSE_NAME,
)


PRODUCT_SCHEMA = pa.schema([
    pa.field("product_id", pa.int32(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("category", pa.string(), nullable=False),
    pa.field("list_price", pa.float64(), nullable=False),
])

REVENUE_SCHEMA = pa.schema([
    pa.field("date", pa.date32(), nullable=False),
    pa.field("product_id", pa.int32(), nullable=False),
    pa.field("units", pa.int32(), nullable=False),
    pa.field("amount", pa.float64(), nullable=False),
])

PRODUCT_ROWS = pa.Table.from_pylist(
    [
        {"product_id": 1, "name": "Pickaxe",       "category": "tools",     "list_price": 19.99},
        {"product_id": 2, "name": "Shovel",        "category": "tools",     "list_price": 14.50},
        {"product_id": 3, "name": "Lantern",       "category": "lighting",  "list_price": 29.95},
        {"product_id": 4, "name": "Compass",       "category": "navigation","list_price": 12.00},
        {"product_id": 5, "name": "Climbing Rope", "category": "safety",    "list_price": 49.00},
    ],
    schema=PRODUCT_SCHEMA,
)

import datetime as _dt

REVENUE_ROWS = pa.Table.from_pylist(
    [
        {"date": _dt.date(2026, 1, 5),  "product_id": 1, "units": 12, "amount": 239.88},
        {"date": _dt.date(2026, 1, 5),  "product_id": 2, "units":  8, "amount": 116.00},
        {"date": _dt.date(2026, 1, 6),  "product_id": 3, "units":  3, "amount":  89.85},
        {"date": _dt.date(2026, 1, 7),  "product_id": 5, "units":  2, "amount":  98.00},
        {"date": _dt.date(2026, 2, 1),  "product_id": 1, "units": 20, "amount": 399.80},
        {"date": _dt.date(2026, 2, 1),  "product_id": 4, "units":  6, "amount":  72.00},
        {"date": _dt.date(2026, 2, 14), "product_id": 3, "units":  4, "amount": 119.80},
    ],
    schema=REVENUE_SCHEMA,
)


def main():
    catalog = load_catalog(
        "lakekeeper",
        **{
            "type": "rest",
            "uri": CATALOG_URL,
            "warehouse": WAREHOUSE_NAME,
            "credential": f"{ADMIN_CLIENT_ID}:{ADMIN_CLIENT_SECRET}",
            "oauth2-server-uri": KEYCLOAK_TOKEN_URL,
            "scope": "lakekeeper",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": S3_REGION,
            "s3.path-style-access": "true",
        },
    )

    print(f"Ensuring namespace '{NAMESPACE_NAME}'...")
    try:
        catalog.create_namespace(NAMESPACE_NAME)
        print("  ✓ created")
    except NamespaceAlreadyExistsError:
        print("  (already exists)")

    for fqn, schema, rows in [
        (PRODUCT_TABLE_FQN, PRODUCT_SCHEMA, PRODUCT_ROWS),
        (REVENUE_TABLE_FQN, REVENUE_SCHEMA, REVENUE_ROWS),
    ]:
        print(f"\nEnsuring table '{fqn}'...")
        try:
            table = catalog.create_table(fqn, schema=schema)
            table.append(rows)
            print(f"  ✓ created and loaded {rows.num_rows} rows")
        except TableAlreadyExistsError:
            table = catalog.load_table(fqn)
            existing = table.scan().to_arrow()
            print(f"  (already exists with {existing.num_rows} rows — leaving untouched)")

    print("\nDone.")


if __name__ == "__main__":
    main()
