"""Spark h2m: authorization-code (PKCE) via Dremio's authmgr-oauth2 AuthManager.

    python spark/h2m.py

The Dremio AuthManager spins up a localhost HTTP listener, prints the
authorization URL, and catches the redirect. Open the URL in your browser
and log in as `peter` (full warehouse access) or `anna` (only finance.product).

Note: authmgr 1.0 instantiates two separate OAuth2 agents — one for the
`initSession` (catalog config discovery) and one for the `catalogSession`
(actual operations) — so you'll see TWO login prompts on first run. Same
browser session works; second one is one click ("Continue").
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from lib.config import CATALOG_URL, WAREHOUSE_NAME

ICEBERG_VERSION = "1.10.0"
AUTHMGR_VERSION = "1.0.0"
PACKAGES = ",".join([
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"com.dremio.iceberg.authmgr:authmgr-oauth2-runtime:{AUTHMGR_VERSION}",
])

# Plain HTTP — Java's keystore doesn't trust the gateway's self-signed cert.
ISSUER = "http://keycloak.localtest.me:30080/realms/iceberg"

spark = (
    SparkSession.builder.appName("workshop-h2m")
    .master("local[*]")
    .config("spark.jars.packages", PACKAGES)
    .config("spark.sql.ansi.enabled", "true")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakekeeper", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakekeeper.type", "rest")
    .config("spark.sql.catalog.lakekeeper.uri", CATALOG_URL)
    .config("spark.sql.catalog.lakekeeper.warehouse", WAREHOUSE_NAME)
    .config(
        "spark.sql.catalog.lakekeeper.rest.auth.type",
        "com.dremio.iceberg.authmgr.oauth2.OAuth2Manager",
    )
    .config("spark.sql.catalog.lakekeeper.rest.auth.oauth2.issuer-url", ISSUER)
    .config("spark.sql.catalog.lakekeeper.rest.auth.oauth2.client-id", "lakekeeper")
    .config("spark.sql.catalog.lakekeeper.rest.auth.oauth2.client-auth", "none")
    .config("spark.sql.catalog.lakekeeper.rest.auth.oauth2.grant-type", "authorization_code")
    .config("spark.sql.catalog.lakekeeper.rest.auth.oauth2.scope", "openid lakekeeper")
    .config("spark.sql.defaultCatalog", "lakekeeper")
    .getOrCreate()
)

spark.sql("SHOW NAMESPACES").show()
spark.sql("SELECT * FROM finance.product").show()
try:
    spark.sql("SELECT * FROM finance.revenue").show()
except Exception as e:
    print(f"revenue blocked: {type(e).__name__}: {e}")
