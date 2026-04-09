"""Demo: OAuth2 Client Credentials Flow (machine-to-machine).

The "client credentials" flow is for machine-to-machine authentication:
the client (Spark, Trino, an ETL job, ...) authenticates with its own
client_id + client_secret. No human is involved.

This script:
  1. Gets a token from Keycloak with grant_type=client_credentials
  2. Decodes the JWT so you can see the claims
  3. Uses the token to list projects in Lakekeeper
"""

import base64
import json

import httpx
import urllib3
from lib.config import (
    KEYCLOAK_TOKEN_URL,
    MANAGEMENT_URL,
    SPARK_CLIENT_ID,
    SPARK_CLIENT_SECRET,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    print("=" * 70)
    print("OAuth2 Client Credentials Flow")
    print("=" * 70)

    # 1. Request a token
    print(f"\n[1/3] POST {KEYCLOAK_TOKEN_URL}")
    print(f"      grant_type=client_credentials  client_id={SPARK_CLIENT_ID}")
    response = httpx.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": SPARK_CLIENT_ID,
            "client_secret": SPARK_CLIENT_SECRET,
            "scope": "lakekeeper",
        },
        verify=False,
    )
    response.raise_for_status()
    token_response = response.json()
    access_token = token_response["access_token"]
    print(f"      ✓ Got token (expires in {token_response['expires_in']}s)")

    # 2. Inspect the JWT
    print("\n[2/3] JWT claims:")
    payload = access_token.split(".")[1]
    payload += "=" * (4 - len(payload) % 4)
    claims = json.loads(base64.b64decode(payload))
    for key in ("iss", "aud", "azp", "sub", "preferred_username", "scope", "exp"):
        if key in claims:
            print(f"        {key:20s}: {claims[key]}")

    # 3. Use the token: list Lakekeeper projects
    print(f"\n[3/3] GET {MANAGEMENT_URL}/v1/project-list")
    response = httpx.get(
        f"{MANAGEMENT_URL}/v1/project-list",
        headers={"Authorization": f"Bearer {access_token}"},
        verify=False,
    )
    print(f"      Status: {response.status_code}")
    print(f"      Response:\n{json.dumps(response.json(), indent=8)}")

    print("\n✓ Done.")


if __name__ == "__main__":
    main()
