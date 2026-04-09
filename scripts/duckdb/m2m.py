"""DuckDB m2m: connect to Lakekeeper's Iceberg REST catalog as airflow-sp-1.

    python duckdb/m2m.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb

from lib.config import (
    AIRFLOW_CLIENT_1_ID,
    AIRFLOW_CLIENT_1_SECRET,
    CATALOG_URL,
    KEYCLOAK_TOKEN_URL,
    WAREHOUSE_NAME,
)

con = duckdb.connect()

con.execute("INSTALL iceberg")
con.execute("INSTALL httpfs")
con.execute("LOAD iceberg")
con.execute("LOAD httpfs")

con.execute(
    f"""
    CREATE OR REPLACE SECRET lakekeeper_oauth (
      TYPE iceberg,
      CLIENT_ID '{AIRFLOW_CLIENT_1_ID}',
      CLIENT_SECRET '{AIRFLOW_CLIENT_1_SECRET}',
      OAUTH2_SCOPE 'lakekeeper',
      OAUTH2_SERVER_URI '{KEYCLOAK_TOKEN_URL}'
    )
    """
)

con.execute(
    f"""
    ATTACH '{WAREHOUSE_NAME}' AS lakekeeper (
      TYPE iceberg,
      ENDPOINT '{CATALOG_URL}',
      SECRET lakekeeper_oauth
    )
    """
)

print("\n--- SHOW ALL TABLES ---")
print(con.execute("SHOW ALL TABLES").fetchdf())

print("\n--- finance.product ---")
print(con.execute("SELECT * FROM lakekeeper.finance.product").fetchdf())

print("\n--- finance.revenue ---")
try:
    print(con.execute("SELECT * FROM lakekeeper.finance.revenue").fetchdf())
except Exception as e:
    print(f"  ✗ {type(e).__name__}: {e}")
