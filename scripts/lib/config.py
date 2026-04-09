"""Shared configuration for workshop scripts."""

import os
import sys
from pathlib import Path

# --- Import shim helper ---------------------------------------------------
# Scripts in subfolders import from `lib.config` — but only when run with the
# scripts/ directory on sys.path. Each script in 00_setup/, oauth/, pyiceberg/,
# trino/, starrocks/ adds the following two lines at the top:
#
#     import sys; from pathlib import Path
#     sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
#
# That makes `from lib.config import ...` work no matter the cwd.

# --- Lakekeeper -----------------------------------------------------------
LAKEKEEPER_URL = "http://lakekeeper.localtest.me:30080"
CATALOG_URL = f"{LAKEKEEPER_URL}/catalog"
MANAGEMENT_URL = f"{LAKEKEEPER_URL}/management"

# --- Keycloak -------------------------------------------------------------
# HTTPS frontend (used by browsers / human flows). Self-signed cert.
KEYCLOAK_URL = "https://keycloak.localtest.me:30443"
# Plain-HTTP token endpoint for scripts and m2m flows. Avoids the self-signed
# cert headache for libraries (e.g. PyIceberg's auth manager) that don't
# expose a way to disable TLS verification on the token request.
KEYCLOAK_TOKEN_URL = "http://keycloak.localtest.me:30080/realms/iceberg/protocol/openid-connect/token"

# --- Bootstrap admin client ----------------------------------------------
ADMIN_CLIENT_ID = "lakehouse-admin"
ADMIN_CLIENT_SECRET = "3VQlLbU6FbtxmIlKPBIXxQnt2KMfGp4G"

# --- OPA bridge (Trino's authz query path) -------------------------------
OPA_BRIDGE_CLIENT_ID = "opa-bridge"
OPA_BRIDGE_CLIENT_SECRET = "RCUlsviRgBaf8BtHvjSe1BGfmRoRS5KH"

# --- Engine service accounts ---------------------------------------------
TRINO_CLIENT_ID = "trino"
TRINO_CLIENT_SECRET = "AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ"

STARROCKS_CLIENT_ID = "starrocks"
STARROCKS_CLIENT_SECRET = "X5IWbfDJBTcU1F3PGZWgxDJwLyuFQmSf"

SPARK_CLIENT_ID = "spark"
SPARK_CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

# --- Application service accounts (Airflow) ------------------------------
# sp-1 has full warehouse modify rights (the "trusted writer" persona)
# sp-2 has table-level SELECT on finance.product only ("restricted reader")
AIRFLOW_CLIENT_1_ID = "airflow-sp-1"
AIRFLOW_CLIENT_1_SECRET = "NZ1yTmbEto9ZejVbtJjJ9hAVVCENblmB"
AIRFLOW_CLIENT_2_ID = "airflow-sp-2"
AIRFLOW_CLIENT_2_SECRET = "7mXZ4Te1ijR0IgaxEaJGr3EhQm60xgAh"

SERVICE_PRINCIPALS = {
    "airflow-sp-1": (AIRFLOW_CLIENT_1_ID, AIRFLOW_CLIENT_1_SECRET),
    "airflow-sp-2": (AIRFLOW_CLIENT_2_ID, AIRFLOW_CLIENT_2_SECRET),
}


def get_sp(name: str | None = None) -> tuple[str, str]:
    """Resolve a service-principal name to (client_id, client_secret).

    Picks (in order): the explicit `name` arg, the WORKSHOP_SP env var, or
    the default `airflow-sp-1`. This is what every m2m engine script uses
    so a workshop attendee can flip between sp-1 and sp-2 to demonstrate
    that authz actually changes the result set.
    """
    sp = name or os.environ.get("WORKSHOP_SP", "airflow-sp-1")
    if sp not in SERVICE_PRINCIPALS:
        raise SystemExit(
            f"Unknown WORKSHOP_SP={sp!r}. "
            f"Valid values: {', '.join(SERVICE_PRINCIPALS)}"
        )
    print(f"[sp] using service principal: {sp}", file=sys.stderr)
    return SERVICE_PRINCIPALS[sp]


# --- Human users (Keycloak realm) ----------------------------------------
# Lakekeeper user IDs are `oidc~<keycloak-sub>` for OIDC-federated users.
PETER_USER_ID = "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
ANNA_USER_ID = "oidc~d223d88c-85b6-4859-b5c5-27f3825e47f6"

# --- SeaweedFS S3 ---------------------------------------------------------
S3_ENDPOINT = "http://s3.localtest.me:30080"
S3_BUCKET = "examples"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "adminadmin"
S3_REGION = "us-east-1"

# --- Workshop data shape -------------------------------------------------
WAREHOUSE_NAME = "demo"
NAMESPACE_NAME = "finance"
TABLE_PRODUCT = "product"
TABLE_REVENUE = "revenue"

PRODUCT_TABLE_FQN = f"{NAMESPACE_NAME}.{TABLE_PRODUCT}"
REVENUE_TABLE_FQN = f"{NAMESPACE_NAME}.{TABLE_REVENUE}"
