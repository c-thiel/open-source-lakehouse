"""Test PyIceberg connection to Lakekeeper with vended credentials."""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pyiceberg.catalog import load_catalog
from auth import get_token
from config import (
    LAKEKEEPER_URL,
    KEYCLOAK_TOKEN_URL,
    SPARK_CLIENT_ID,
    SPARK_CLIENT_SECRET,
)


def main():
    print("Getting token for spark service account...")
    token = get_token(SPARK_CLIENT_ID, SPARK_CLIENT_SECRET)

    print("Loading PyIceberg catalog...")
    catalog = load_catalog(
        "lakekeeper",
        type="rest",
        uri=f"{LAKEKEEPER_URL}/catalog",
        warehouse="demo",
        token=token,
    )

    print("\n--- List namespaces ---")
    namespaces = catalog.list_namespaces()
    for ns in namespaces:
        print(f"  {ns}")

    print("\n--- Create namespace ---")
    try:
        catalog.create_namespace("workshop")
        print("  Created 'workshop'")
    except Exception as e:
        print(f"  {e}")

    print("\n--- Create table ---")
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, StringType, NestedField

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )

    try:
        table = catalog.create_table("workshop.test_pyiceberg", schema=schema)
        print(f"  Created table: {table.identifier}")
    except Exception as e:
        print(f"  {e}")

    print("\n--- List tables ---")
    tables = catalog.list_tables("workshop")
    for t in tables:
        print(f"  {t}")

    print("\n--- Write data ---")
    import pyarrow as pa

    table = catalog.load_table("workshop.test_pyiceberg")
    df = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "name": ["alice", "bob", "charlie"],
    })
    try:
        table.append(df)
        print("  Appended 3 rows")
    except Exception as e:
        print(f"  {e}")

    print("\n--- Read data ---")
    try:
        scan = table.scan()
        result = scan.to_arrow()
        print(result.to_pandas())
    except Exception as e:
        print(f"  {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
