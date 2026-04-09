"""Trino machine-to-machine with automatic token refresh.

trino-python-client's `JWTAuthentication(token)` takes a static token and
does NOT refresh it — long-running jobs would start failing once the
token expires. This script plugs a custom requests AuthBase that fetches
a fresh client_credentials token before each request when needed.

Switch service principal via WORKSHOP_SP env var (default airflow-sp-1).
"""

import threading
import time

import httpx
import urllib3
from lib.config import (
    KEYCLOAK_TOKEN_URL,
    NAMESPACE_NAME,
    PRODUCT_TABLE_FQN,
    REVENUE_TABLE_FQN,
    get_sp,
)
from requests.auth import AuthBase
from trino.auth import Authentication
from trino.dbapi import connect

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TRINO_HOST = "trino.localtest.me"
TRINO_PORT = 30443
CATALOG = "lakekeeper"
REFRESH_MARGIN_SECONDS = 30


class ClientCredentialsTokenProvider:
    """Fetches and caches an access token via OAuth2 client_credentials."""

    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str):
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0

    def get_token(self) -> str:
        with self._lock:
            if self._token is None or time.monotonic() >= self._expires_at:
                self._refresh()
            assert self._token is not None
            return self._token

    def _refresh(self) -> None:
        response = httpx.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": self._scope,
            },
            verify=False,
        )
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        self._expires_at = (
            time.monotonic() + payload["expires_in"] - REFRESH_MARGIN_SECONDS
        )
        print(f"  ↻ Fetched new token (expires in {payload['expires_in']}s)")


class _RefreshingBearerAuth(AuthBase):
    def __init__(self, provider: ClientCredentialsTokenProvider):
        self._provider = provider

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self._provider.get_token()}"
        return request


class ClientCredentialsAuthentication(Authentication):
    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str):
        self._provider = ClientCredentialsTokenProvider(
            token_url, client_id, client_secret, scope
        )

    def set_http_session(self, http_session):
        http_session.auth = _RefreshingBearerAuth(self._provider)
        return http_session

    def get_exceptions(self):
        return ()


def query(cur, sql: str) -> None:
    print(f"\n--- {sql} ---")
    try:
        cur.execute(sql)
        for row in cur.fetchall():
            print(f"  {row}")
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")


def main():
    sp_id, sp_secret = get_sp()

    auth = ClientCredentialsAuthentication(
        token_url=KEYCLOAK_TOKEN_URL,
        client_id=sp_id,
        client_secret=sp_secret,
        scope="trino",
    )

    print(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT} as {sp_id}...")
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        auth=auth,
        http_scheme="https",
        verify=False,
        catalog=CATALOG,
        schema=NAMESPACE_NAME,
    )
    cur = conn.cursor()

    query(cur, "SHOW CATALOGS")
    query(cur, f"SHOW SCHEMAS FROM {CATALOG}")
    query(cur, f"SHOW TABLES FROM {CATALOG}.{NAMESPACE_NAME}")
    query(cur, f"SELECT * FROM {CATALOG}.{PRODUCT_TABLE_FQN}")
    query(cur, f"SELECT * FROM {CATALOG}.{REVENUE_TABLE_FQN}")

    print("\nDone. Long-running queries will silently refresh tokens as needed.")


if __name__ == "__main__":
    main()
