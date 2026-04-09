"""OAuth2 token helper for Keycloak."""

import httpx
from lib.config import ADMIN_CLIENT_ID, ADMIN_CLIENT_SECRET, KEYCLOAK_TOKEN_URL


def get_token(client_id: str, client_secret: str, scope: str = "lakekeeper") -> str:
    """Fetch an access token via the OAuth2 client_credentials flow."""
    response = httpx.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        verify=False,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def admin_headers() -> dict[str, str]:
    """Authorization headers using the bootstrap admin client."""
    token = get_token(ADMIN_CLIENT_ID, ADMIN_CLIENT_SECRET)
    return {"Authorization": f"Bearer {token}"}
