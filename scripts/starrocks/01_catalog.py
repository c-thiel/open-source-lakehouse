"""Bootstrap StarRocks: Iceberg catalog with JWT passthrough + OAuth2 SI.

Run once. Idempotent (drops + recreates the catalog and SI).

What it does:
  1. Connects to StarRocks as root (native auth).
  2. Creates an external Iceberg catalog `lakekeeper` with security=JWT.
     The starrocks SP credential is used only for the bootstrap fetchConfig
     call; user queries forward the user's own JWT to Lakekeeper.
  3. Grants USAGE + SELECT on this catalog to the `public` role. Every
     authenticated user (local + virtual users from the SI below) inherits
     `public`, so StarRocks-side authz becomes a no-op and Lakekeeper alone
     enforces per-table access against the forwarded JWT (StarRocks 4.0
     catalog-centric pattern).
  4. Creates an OAuth2 security integration `keycloak_oauth2` and puts it
     at the head of authentication_chain. This drives the browser-based
     auth code flow for h2m users — but only via JDBC clients that ship
     the StarRocks `authentication_oauth2_client` plugin (e.g. DBeaver).
     See starrocks/03_h2m_dbeaver.md.

About the dual Keycloak URLs:
  - auth_server_url MUST be the externally-reachable HTTPS endpoint
    (Keycloak's issuer). The user's browser visits this URL.
  - token_server_url + jwks_url use the in-cluster plain-HTTP alias
    because the FE itself fetches them from inside the kind cluster.
"""

import pymysql
from lib.config import (
    CATALOG_URL,
    KEYCLOAK_URL,
    STARROCKS_CLIENT_ID,
    STARROCKS_CLIENT_SECRET,
    WAREHOUSE_NAME,
)

CATALOG_NAME = "lakekeeper"

KEYCLOAK_INCLUSTER_TOKEN_URL = (
    "http://keycloak.localtest.me:30080/realms/iceberg/protocol/openid-connect/token"
)
KEYCLOAK_INCLUSTER_JWKS_URL = (
    "http://keycloak.localtest.me:30080/realms/iceberg/protocol/openid-connect/certs"
)
KEYCLOAK_BROWSER_AUTH_URL = (
    f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/auth"
)
KEYCLOAK_ISSUER = f"{KEYCLOAK_URL}/realms/iceberg"
STARROCKS_REDIRECT_URL = "http://starrocks.localtest.me:30080/api/oauth2"

STARROCKS_HOST = "127.0.0.1"
STARROCKS_MYSQL_PORT = 30930


def main():
    print(f"Connecting to StarRocks at {STARROCKS_HOST}:{STARROCKS_MYSQL_PORT}...")
    conn = pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_MYSQL_PORT,
        user="root",
        password="",
    )
    cur = conn.cursor()

    print(f"\nDropping existing catalog '{CATALOG_NAME}' (if any)...")
    try:
        cur.execute(f"DROP CATALOG IF EXISTS `{CATALOG_NAME}`")
    except pymysql.err.OperationalError as e:
        print(f"  (ignored) {e}")

    print(f"\nCreating catalog '{CATALOG_NAME}' with JWT passthrough...")
    cur.execute(
        f"""
        CREATE EXTERNAL CATALOG `{CATALOG_NAME}`
        PROPERTIES (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "iceberg.catalog.uri" = "{CATALOG_URL}",
            "iceberg.catalog.warehouse" = "{WAREHOUSE_NAME}",
            "iceberg.catalog.security" = "JWT",
            "iceberg.catalog.oauth2.server-uri" = "{KEYCLOAK_INCLUSTER_TOKEN_URL}",
            "iceberg.catalog.oauth2.credential" = "{STARROCKS_CLIENT_ID}:{STARROCKS_CLIENT_SECRET}",
            "iceberg.catalog.oauth2.scope" = "lakekeeper",
            "iceberg.catalog.token-exchange-enabled" = "false",
            "aws.s3.endpoint" = "http://s3.localtest.me:30080",
            "aws.s3.region" = "us-east-1",
            "aws.s3.enable_path_style_access" = "true",
            "enable_iceberg_metadata_disk_cache" = "false"
        )
    """
    )
    print("  ✓ Catalog created")

    print(f"\nGranting catalog access on '{CATALOG_NAME}' to role lk_reader...")
    cur.execute("CREATE ROLE IF NOT EXISTS lk_reader")
    cur.execute(f"GRANT USAGE ON CATALOG `{CATALOG_NAME}` TO ROLE lk_reader")
    cur.execute(f"SET CATALOG `{CATALOG_NAME}`")
    cur.execute("GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE lk_reader")
    cur.execute("SET CATALOG default_catalog")
    print("  ✓ lk_reader role can USAGE the catalog and SELECT all tables")

    print("\nCreating OAuth2 security integration 'keycloak_oauth2'...")
    try:
        cur.execute("DROP SECURITY INTEGRATION keycloak_oauth2")
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        print(f"  (no existing integration to drop) {e}")
    cur.execute(
        f"""
        CREATE SECURITY INTEGRATION keycloak_oauth2 PROPERTIES (
            "type"              = "authentication_oauth2",
            "auth_server_url"   = "{KEYCLOAK_BROWSER_AUTH_URL}",
            "token_server_url"  = "{KEYCLOAK_INCLUSTER_TOKEN_URL}",
            "client_id"         = "{STARROCKS_CLIENT_ID}",
            "client_secret"     = "{STARROCKS_CLIENT_SECRET}",
            "redirect_url"      = "{STARROCKS_REDIRECT_URL}",
            "jwks_url"          = "{KEYCLOAK_INCLUSTER_JWKS_URL}",
            "principal_field"   = "preferred_username",
            "required_issuer"   = "{KEYCLOAK_ISSUER}",
            "required_audience" = "{STARROCKS_CLIENT_ID}"
        )
    """
    )
    cur.execute(
        'ADMIN SET FRONTEND CONFIG ("authentication_chain" = "keycloak_oauth2,native")'
    )
    print("  ✓ Security integration created and added to authentication_chain")

    print("\n--- SHOW CATALOGS ---")
    cur.execute("SHOW CATALOGS")
    for row in cur.fetchall():
        print(f"  {row}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
