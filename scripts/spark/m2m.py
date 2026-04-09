"""Spark m2m: client_credentials against Keycloak via the Iceberg REST catalog.

    python spark/m2m.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from lib.config import (
    CATALOG_URL,
    KEYCLOAK_TOKEN_URL,
    SPARK_CLIENT_ID,
    SPARK_CLIENT_SECRET,
    WAREHOUSE_NAME,
)

ICEBERG_VERSION = "1.10.0"
PACKAGES = ",".join([
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
])

spark = (
    SparkSession.builder.appName("workshop-m2m")
    .master("local[*]")
    .config("spark.jars.packages", PACKAGES)
    .config("spark.sql.ansi.enabled", "true")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakekeeper", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakekeeper.type", "rest")
    .config("spark.sql.catalog.lakekeeper.uri", CATALOG_URL)
    .config("spark.sql.catalog.lakekeeper.warehouse", WAREHOUSE_NAME)
    .config("spark.sql.catalog.lakekeeper.credential", f"{SPARK_CLIENT_ID}:{SPARK_CLIENT_SECRET}")
    .config("spark.sql.catalog.lakekeeper.oauth2-server-uri", KEYCLOAK_TOKEN_URL)
    .config("spark.sql.catalog.lakekeeper.scope", "lakekeeper")
    .config("spark.sql.defaultCatalog", "lakekeeper")
    .getOrCreate()
)

spark.sql("SHOW NAMESPACES").show()
spark.sql("SELECT * FROM finance.product").show()
try:
    spark.sql("SELECT * FROM finance.revenue").show()
except Exception as e:
    print(f"revenue blocked: {type(e).__name__}: {e}")
