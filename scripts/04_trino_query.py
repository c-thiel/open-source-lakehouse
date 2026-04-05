"""Run a test query on Trino via OAuth2 browser login."""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from trino.dbapi import connect
from trino.auth import OAuth2Authentication, CompositeRedirectHandler, WebBrowserRedirectHandler


TRINO_HOST = "trino.localhost"
TRINO_PORT = 30443


def main():
    redirect_handler = CompositeRedirectHandler([WebBrowserRedirectHandler()])

    print(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    print("A browser window will open for Keycloak login.")
    print("Login as: peter / iceberg\n")

    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        auth=OAuth2Authentication(redirect_handler),
        http_scheme="https",
        verify=False,
        catalog="lakekeeper",
    )

    cur = conn.cursor()

    print("--- SHOW CATALOGS ---")
    cur.execute("SHOW CATALOGS")
    for row in cur.fetchall():
        print(f"  {row[0]}")

    print("\n--- SHOW SCHEMAS ---")
    try:
        cur.execute("SHOW SCHEMAS")
        for row in cur.fetchall():
            print(f"  {row[0]}")
    except Exception as e:
        print(f"  Error: {e}")

    print("\n--- CREATE SCHEMA ---")
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS workshop")
        print("  Schema 'workshop' created (or already exists)")
    except Exception as e:
        print(f"  Error: {e}")

    print("\n--- CREATE TABLE ---")
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS workshop.test (
                id INTEGER,
                name VARCHAR
            )
        """)
        print("  Table 'test' created (or already exists)")
    except Exception as e:
        print(f"  Error: {e}")

    print("\n--- INSERT DATA ---")
    try:
        cur.execute("""
            INSERT INTO workshop.test VALUES (1, 'hello'), (2, 'world')
        """)
        print("  Inserted rows")
    except Exception as e:
        print(f"  Error: {e}")

    print("\n--- SELECT DATA ---")
    try:
        cur.execute("SELECT * FROM workshop.test")
        for row in cur.fetchall():
            print(f"  {row}")
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
