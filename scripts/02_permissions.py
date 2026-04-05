"""Grant permissions to users and technical clients.

Grants:
- opa-bridge: server operator (queries Lakekeeper authorization API for OPA policies)
- trino: server operator (Trino's service account needs catalog-level access)
- peter: server admin + project admin (human user)
- spark: project-level create/modify/select/describe
"""

import httpx
from auth import admin_headers
from config import MANAGEMENT_URL, PETER_USER_ID, TRINO_CLIENT_ID, SPARK_CLIENT_ID


def permissions_url(scope: str) -> str:
    """Build the permissions URL."""
    return f"{MANAGEMENT_URL}/v1/permissions/{scope}/assignments"


def get_existing_assignments(headers: dict, scope: str) -> set[str]:
    """Get existing assignments as a set of (type, user) tuples."""
    response = httpx.get(permissions_url(scope), headers=headers, verify=False)
    response.raise_for_status()
    return {
        (a["type"], a["user"]) for a in response.json().get("assignments", [])
    }


def write_assignments(
    headers: dict,
    scope: str,
    writes: list[dict],
) -> None:
    """Write permission assignments, skipping those that already exist."""
    existing = get_existing_assignments(headers, scope)
    new_writes = [
        w for w in writes if (w["type"], w["user"]) not in existing
    ]

    if not new_writes:
        print(f"  {scope}: all assignments already exist — nothing to do.")
        return

    response = httpx.post(
        permissions_url(scope),
        headers=headers,
        json={"writes": new_writes},
        verify=False,
    )
    response.raise_for_status()
    for w in new_writes:
        print(f"  {scope}: granted {w['type']} to {w['user']}")


def ensure_user_exists(headers: dict, client_id: str, client_secret: str) -> None:
    """Make a catalog config call so the service account appears in Lakekeeper."""
    from auth import get_token
    from config import CATALOG_URL

    token = get_token(client_id, client_secret)
    httpx.get(
        f"{CATALOG_URL}/v1/config",
        headers={"Authorization": f"Bearer {token}"},
        verify=False,
    )


def self_provision_with_token(client_id: str, client_secret: str) -> None:
    """Self-provision a service account using its own token."""
    from auth import get_token
    try:
        token = get_token(client_id, client_secret)
        response = httpx.post(
            f"{MANAGEMENT_URL}/v1/user",
            headers={"Authorization": f"Bearer {token}"},
            json={},
            verify=False,
        )
        if response.is_success:
            print(f"  {client_id}: self-provisioned.")
        else:
            print(f"  {client_id}: returned {response.status_code} — likely already exists.")
    except Exception as e:
        print(f"  {client_id}: self-provision failed ({e}) — continuing.")


def self_provision(headers: dict) -> None:
    """Self-provision the admin user. Idempotent — ignores errors."""
    try:
        response = httpx.post(
            f"{MANAGEMENT_URL}/v1/user",
            headers=headers,
            json={},
            verify=False,
        )
        if response.is_success:
            print("  Admin user self-provisioned.")
        else:
            print(f"  Self-provision returned {response.status_code} — likely already exists.")
    except Exception as e:
        print(f"  Self-provision failed ({e}) — continuing.")


def main():
    headers = admin_headers()

    # Self-provision the script's own user
    print("Self-provisioning admin user...")
    self_provision(headers)

    # Register and grant opa-bridge server operator
    print("\nRegistering opa-bridge user...")
    from config import OPA_BRIDGE_CLIENT_ID, OPA_BRIDGE_CLIENT_SECRET
    ensure_user_exists(headers, OPA_BRIDGE_CLIENT_ID, OPA_BRIDGE_CLIENT_SECRET)
    self_provision_with_token(OPA_BRIDGE_CLIENT_ID, OPA_BRIDGE_CLIENT_SECRET)

    # Ensure technical users exist in Lakekeeper by triggering a catalog call
    print("\nRegistering technical users...")
    for client_id, client_secret in [
        (TRINO_CLIENT_ID, __import__("config").TRINO_CLIENT_SECRET),
        (SPARK_CLIENT_ID, __import__("config").SPARK_CLIENT_SECRET),
    ]:
        ensure_user_exists(headers, client_id, client_secret)
        print(f"  {client_id}: registered")

    # Get all user IDs
    response = httpx.get(f"{MANAGEMENT_URL}/v1/user", headers=headers, verify=False)
    response.raise_for_status()
    users = {u["name"]: u["id"] for u in response.json().get("users", [])}

    # Grant opa-bridge server operator
    opa_user_id = users.get("service-account-opa-bridge")
    if opa_user_id:
        print("\nGranting permissions to opa-bridge...")
        write_assignments(headers, "server", [
            {"type": "operator", "user": opa_user_id},
        ])
    else:
        print("\n  WARNING: opa-bridge user not found")

    # Grant peter server admin + project admin
    print("\nGranting permissions to peter...")
    write_assignments(headers, "server", [
        {"type": "admin", "user": PETER_USER_ID},
    ])
    write_assignments(headers, "project", [
        {"type": "project_admin", "user": PETER_USER_ID},
    ])

    # Get technical user IDs from Lakekeeper
    response = httpx.get(f"{MANAGEMENT_URL}/v1/user", headers=headers, verify=False)
    response.raise_for_status()
    users = {u["name"]: u["id"] for u in response.json().get("users", [])}

    # Grant trino server operator (needed for catalog access via OPA)
    trino_user_id = users.get("service-account-trino")
    if trino_user_id:
        print("\nGranting server operator to trino...")
        write_assignments(headers, "server", [
            {"type": "operator", "user": trino_user_id},
        ])

    # Grant spark project-level permissions
    spark_user_id = users.get("service-account-spark")
    if spark_user_id:
        print("\nGranting permissions to spark...")
        write_assignments(headers, "project", [
            {"type": "create", "user": spark_user_id},
            {"type": "modify", "user": spark_user_id},
            {"type": "select", "user": spark_user_id},
            {"type": "describe", "user": spark_user_id},
        ])
    else:
        print("\n  WARNING: spark user not found")

    print("\nDone.")


if __name__ == "__main__":
    main()
