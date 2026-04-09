"""Bootstrap Lakekeeper once with the admin client. Idempotent."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from typing import cast

import httpx
import urllib3

from lib.auth import admin_headers
from lib.config import MANAGEMENT_URL

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    headers = admin_headers()

    info = cast(
        dict[str, object],
        httpx.get(f"{MANAGEMENT_URL}/v1/info", headers=headers, verify=False).json(),
    )
    if info.get("bootstrapped"):
        print("Lakekeeper already bootstrapped.")
        return

    print("Bootstrapping Lakekeeper...")
    response = httpx.post(
        f"{MANAGEMENT_URL}/v1/bootstrap",
        headers=headers,
        json={"accept-terms-of-use": True},
        verify=False,
    )
    _ = response.raise_for_status()
    print("✓ Bootstrapped.")


if __name__ == "__main__":
    main()
