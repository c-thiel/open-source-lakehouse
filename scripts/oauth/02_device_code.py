"""Demo: OAuth2 Device Code Flow (human-to-machine).

The "device code" flow is for human authentication on devices that cannot
host a web browser themselves (CLIs, headless scripts, IoT, ...). The user
opens a URL on a separate device (phone, laptop) and enters a short code.

This script:
  1. Asks Keycloak for a device code
  2. Prints a URL for you to open in a browser
  3. Polls until you've logged in
  4. Decodes the JWT so you can see the claims
  5. Uses the token to list projects in Lakekeeper
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import base64
import json
import time

import httpx

from lib.config import KEYCLOAK_URL, MANAGEMENT_URL


HUMAN_CLIENT_ID = "lakekeeper"
DEVICE_CODE_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/auth/device"
TOKEN_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/token"


def main():
    print("=" * 70)
    print("OAuth2 Device Code Flow")
    print("=" * 70)

    print(f"\n[1/4] POST {DEVICE_CODE_URL}")
    print(f"      client_id={HUMAN_CLIENT_ID}")
    response = httpx.post(
        DEVICE_CODE_URL,
        data={"client_id": HUMAN_CLIENT_ID, "scope": "openid lakekeeper"},
        verify=False,
    )
    response.raise_for_status()
    device = response.json()
    print(f"      ✓ Got device_code, user_code={device['user_code']}")

    print("\n[2/4] Open this URL in your browser:")
    print(f"      {device['verification_uri_complete']}")
    print("      Login as: peter / iceberg  (or anna / iceberg)")

    print(f"\n[3/4] Polling {TOKEN_URL}...")
    interval = device.get("interval", 5)
    while True:
        time.sleep(interval)
        response = httpx.post(
            TOKEN_URL,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": device["device_code"],
                "client_id": HUMAN_CLIENT_ID,
            },
            verify=False,
        )
        if response.status_code == 200:
            token_response = response.json()
            access_token = token_response["access_token"]
            print(f"      ✓ Got token (expires in {token_response['expires_in']}s)")
            break
        error = response.json().get("error")
        if error == "authorization_pending":
            print("      ...waiting for browser login")
            continue
        if error == "slow_down":
            interval += 5
            continue
        raise RuntimeError(f"Device flow failed: {response.json()}")

    print("\n      JWT claims:")
    payload = access_token.split(".")[1]
    payload += "=" * (4 - len(payload) % 4)
    claims = json.loads(base64.b64decode(payload))
    for key in ("iss", "aud", "azp", "sub", "preferred_username", "scope", "exp"):
        if key in claims:
            print(f"        {key:20s}: {claims[key]}")

    print(f"\n[4/4] GET {MANAGEMENT_URL}/v1/project-list")
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
