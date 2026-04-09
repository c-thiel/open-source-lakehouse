"""Grant Lakekeeper permissions for the workshop authz story.

Three tiers:

  1. Server operators (needed by their respective engines for catalog wiring):
       - opa-bridge        (operator)     ← Trino's OPA authz query path
       - trino             (operator)
       - starrocks         (operator)     ← + project describe for fetchConfig

  2. Warehouse ownership / modify:
       - peter                              (ownership — the human owner)
       - spark, trino, starrocks, airflow-sp-1   (modify — trusted writers)

  3. Table-level SELECT on finance.product ONLY (the restricted set):
       - airflow-sp-2
       - anna
     They get *no* access to finance.revenue, so a query against revenue
     must fail with 'forbidden'. That's the demo.
"""

import httpx
import urllib3
from lib.auth import admin_headers, get_token
from lib.config import (
    AIRFLOW_CLIENT_1_ID,
    AIRFLOW_CLIENT_1_SECRET,
    AIRFLOW_CLIENT_2_ID,
    AIRFLOW_CLIENT_2_SECRET,
    ANNA_USER_ID,
    CATALOG_URL,
    MANAGEMENT_URL,
    NAMESPACE_NAME,
    OPA_BRIDGE_CLIENT_ID,
    OPA_BRIDGE_CLIENT_SECRET,
    PETER_USER_ID,
    SPARK_CLIENT_ID,
    SPARK_CLIENT_SECRET,
    STARROCKS_CLIENT_ID,
    STARROCKS_CLIENT_SECRET,
    TABLE_PRODUCT,
    TRINO_CLIENT_ID,
    TRINO_CLIENT_SECRET,
    WAREHOUSE_NAME,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ensure_user_exists(client_id: str, client_secret: str) -> None:
    """Trigger a catalog config call so the SP appears in Lakekeeper's user list."""
    token = get_token(client_id, client_secret)
    httpx.get(
        f"{CATALOG_URL}/v1/config",
        headers={"Authorization": f"Bearer {token}"},
        verify=False,
    )


def self_provision(headers: dict) -> None:
    httpx.post(f"{MANAGEMENT_URL}/v1/user", headers=headers, json={}, verify=False)


def self_provision_with_token(client_id: str, client_secret: str) -> None:
    try:
        token = get_token(client_id, client_secret)
        httpx.post(
            f"{MANAGEMENT_URL}/v1/user",
            headers={"Authorization": f"Bearer {token}"},
            json={},
            verify=False,
        )
    except Exception:
        pass


def list_users(headers: dict) -> dict[str, str]:
    response = httpx.get(f"{MANAGEMENT_URL}/v1/user", headers=headers, verify=False)
    response.raise_for_status()
    return {u["name"]: u["id"] for u in response.json().get("users", [])}


def find_warehouse_id(headers: dict, name: str) -> str:
    response = httpx.get(
        f"{MANAGEMENT_URL}/v1/warehouse", headers=headers, verify=False
    )
    response.raise_for_status()
    for wh in response.json().get("warehouses", []):
        if wh.get("name") == name:
            return wh["id"]
    raise SystemExit(f"Warehouse {name!r} not found — run 02_warehouse.py first.")


def find_table_id(headers: dict, warehouse_id: str, namespace: str, table: str) -> str:
    """Resolve a table's UUID via Lakekeeper's fuzzy search endpoint."""
    response = httpx.post(
        f"{MANAGEMENT_URL}/v1/warehouse/{warehouse_id}/search-tabular",
        headers=headers,
        json={"search": table},
        verify=False,
    )
    response.raise_for_status()
    for entry in response.json().get("tabulars", []):
        if (
            entry.get("namespace-name") == [namespace]
            and entry.get("tabular-name") == table
            and entry["tabular-id"]["type"] == "table"
        ):
            return entry["tabular-id"]["id"]
    raise SystemExit(
        f"Table {namespace}.{table} not found in warehouse {warehouse_id} — run 03_data.py first."
    )


def write_assignments(headers: dict, url: str, writes: list[dict]) -> None:
    existing_resp = httpx.get(url, headers=headers, verify=False)
    existing_resp.raise_for_status()
    existing = {
        (a["type"], a["user"]) for a in existing_resp.json().get("assignments", [])
    }
    new_writes = [w for w in writes if (w["type"], w["user"]) not in existing]
    if not new_writes:
        print("  (all assignments already exist)")
        return
    response = httpx.post(
        url, headers=headers, json={"writes": new_writes}, verify=False
    )
    response.raise_for_status()
    for w in new_writes:
        print(f"  ✓ {w['type']} → {w['user']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    headers = admin_headers()

    # 1. Self-provision admin + technical users so they appear in /v1/user
    print("Registering users in Lakekeeper...")
    self_provision(headers)
    for client_id, client_secret in [
        (OPA_BRIDGE_CLIENT_ID, OPA_BRIDGE_CLIENT_SECRET),
        (TRINO_CLIENT_ID, TRINO_CLIENT_SECRET),
        (STARROCKS_CLIENT_ID, STARROCKS_CLIENT_SECRET),
        (SPARK_CLIENT_ID, SPARK_CLIENT_SECRET),
        (AIRFLOW_CLIENT_1_ID, AIRFLOW_CLIENT_1_SECRET),
        (AIRFLOW_CLIENT_2_ID, AIRFLOW_CLIENT_2_SECRET),
    ]:
        ensure_user_exists(client_id, client_secret)
        self_provision_with_token(client_id, client_secret)
        print(f"  registered {client_id}")
    users = list_users(headers)

    def uid(name: str) -> str:
        u = users.get(name)
        if not u:
            raise SystemExit(f"User {name!r} not registered in Lakekeeper.")
        return u

    sp_trino = uid(f"service-account-{TRINO_CLIENT_ID}")
    sp_starr = uid(f"service-account-{STARROCKS_CLIENT_ID}")
    sp_spark = uid(f"service-account-{SPARK_CLIENT_ID}")
    sp_opa = uid(f"service-account-{OPA_BRIDGE_CLIENT_ID}")
    sp_air1 = uid(f"service-account-{AIRFLOW_CLIENT_1_ID}")
    sp_air2 = uid(f"service-account-{AIRFLOW_CLIENT_2_ID}")

    server_url = f"{MANAGEMENT_URL}/v1/permissions/server/assignments"
    project_url = f"{MANAGEMENT_URL}/v1/permissions/project/assignments"

    # 2. Server-level: operators only
    print("\nServer operators (engine catalog wiring)...")
    write_assignments(
        headers,
        server_url,
        [
            {"type": "operator", "user": sp_opa},
            {"type": "operator", "user": sp_trino},
            {"type": "operator", "user": sp_starr},
        ],
    )

    # StarRocks bootstrap fetchConfig needs project-level describe to resolve
    # the warehouse before any user is logged in. This is the ONE project-
    # level grant that survives — it's a structural requirement, not a
    # user-level permission.
    print("\nStarRocks project describe (bootstrap fetchConfig)...")
    write_assignments(
        headers,
        project_url,
        [
            {"type": "describe", "user": sp_starr},
        ],
    )

    # 3. Warehouse modify for the trusted set
    warehouse_id = find_warehouse_id(headers, WAREHOUSE_NAME)
    warehouse_url = (
        f"{MANAGEMENT_URL}/v1/permissions/warehouse/{warehouse_id}/assignments"
    )

    print(f"\nWarehouse '{WAREHOUSE_NAME}' ownership + modify...")
    write_assignments(
        headers,
        warehouse_url,
        [
            {"type": "ownership", "user": PETER_USER_ID},
            {"type": "modify", "user": sp_spark},
            {"type": "modify", "user": sp_trino},
            {"type": "modify", "user": sp_starr},
            {"type": "modify", "user": sp_air1},
        ],
    )

    # 4. Table-level SELECT on finance.product for the restricted set
    table_id = find_table_id(headers, warehouse_id, NAMESPACE_NAME, TABLE_PRODUCT)
    table_url = (
        f"{MANAGEMENT_URL}/v1/permissions/warehouse/{warehouse_id}"
        f"/table/{table_id}/assignments"
    )

    print(f"\nTable '{NAMESPACE_NAME}.{TABLE_PRODUCT}' select (restricted set)...")
    write_assignments(
        headers,
        table_url,
        [
            {"type": "select", "user": sp_air2},
            {"type": "select", "user": ANNA_USER_ID},
        ],
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
