"""Bootstrap Lakekeeper if not already bootstrapped."""

import httpx
from auth import admin_headers
from config import MANAGEMENT_URL


def main():
    headers = admin_headers()

    # Check if already bootstrapped
    response = httpx.get(f"{MANAGEMENT_URL}/v1/info", headers=headers, verify=False)
    response.raise_for_status()
    info = response.json()

    if info.get("bootstrapped"):
        print("Lakekeeper is already bootstrapped — nothing to do.")
        return

    # Bootstrap
    response = httpx.post(
        f"{MANAGEMENT_URL}/v1/bootstrap",
        headers=headers,
        json={"accept-terms-of-use": True},
        verify=False,
    )
    response.raise_for_status()
    print("Lakekeeper bootstrapped successfully.")

    # Verify
    response = httpx.get(f"{MANAGEMENT_URL}/v1/info", headers=headers, verify=False)
    response.raise_for_status()
    print(f"Server info: {response.json()}")


if __name__ == "__main__":
    main()
