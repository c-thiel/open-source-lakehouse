"""OAuth2 token helper."""

import httpx
from config import KEYCLOAK_TOKEN_URL


def get_token(client_id: str, client_secret: str, scope: str = "lakekeeper") -> str:
    """Get an access token from Keycloak using client credentials flow."""
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


def admin_headers() -> dict:
    """Get authorization headers using the admin client."""
    from config import ADMIN_CLIENT_ID, ADMIN_CLIENT_SECRET

    token = get_token(ADMIN_CLIENT_ID, ADMIN_CLIENT_SECRET)
    return {"Authorization": f"Bearer {token}"}
