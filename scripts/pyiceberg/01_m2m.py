"""PyIceberg machine-to-machine: client_credentials with switchable SP.

Pick the service principal via the WORKSHOP_SP env var (default airflow-sp-1):

    WORKSHOP_SP=airflow-sp-1 python pyiceberg/01_m2m.py   # full warehouse modify
    WORKSHOP_SP=airflow-sp-2 python pyiceberg/01_m2m.py   # SELECT on product only

The two runs read both finance.product and finance.revenue. With sp-2, the
revenue read fails with 'forbidden' — that's the authz demo.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pyiceberg.catalog import load_catalog

from lib.config import (
    CATALOG_URL,
    KEYCLOAK_TOKEN_URL,
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
    WAREHOUSE_NAME,
    get_sp,
)


def read_table(catalog, fqn: str) -> None:
    print(f"\n--- {fqn} ---")
    try:
        table = catalog.load_table(fqn)
        df = table.scan().to_arrow().to_pandas()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")


def main():
    sp_id, sp_secret = get_sp()

    print(f"\nLoading PyIceberg catalog as {sp_id} (PyIceberg fetches its own token)...")
    catalog = load_catalog(
        "lakekeeper",
        type="rest",
        uri=CATALOG_URL,
        warehouse=WAREHOUSE_NAME,
        credential=f"{sp_id}:{sp_secret}",
        **{
            "oauth2-server-uri": KEYCLOAK_TOKEN_URL,
            "scope": "lakekeeper",
            # Self-signed Keycloak cert.
            "ssl": {"cabundle": False},
        },
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
