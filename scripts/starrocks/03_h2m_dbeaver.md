# StarRocks h2m — connect with DBeaver

There is no Python script for the StarRocks human-to-machine flow. The
browser-driven OAuth2 against StarRocks requires the
`authentication_oauth2_client` MySQL protocol plugin, which only exists as
a JDBC implementation in StarRocks' own repo:

  https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-jdbc-oauth2-plugin

Stock Oracle MySQL clients (mysql CLI 9.x, mysql-connector-python) do
**not** ship this plugin and immediately drop the connection when StarRocks
tries to negotiate it. The only practical option for h2m today is a
JDBC-based client.

## Setup

1. Install DBeaver (any edition).
2. Build the StarRocks plugin jar:

   ```bash
   git clone --depth 1 https://github.com/StarRocks/starrocks
   cd starrocks/contrib/starrocks-jdbc-oauth2-plugin
   javac -cp <path-to-mysql-connector-j.jar> AuthenticationOAuth2Client.java
   jar cf starrocks-oauth2-plugin.jar AuthenticationOAuth2Client.class
   ```
3. In DBeaver: **Database → Driver Manager → MySQL → Edit → Libraries**.
   Add `starrocks-oauth2-plugin.jar` next to `mysql-connector-j-x.y.z.jar`.

## Connection

**Database → New Connection → MySQL**

- Host: `127.0.0.1`
- Port: `30930`
- Database: leave blank
- Username: `peter`   (or `anna`)
- Password: leave blank

**Driver properties**

| Property                          | Value                          |
|-----------------------------------|--------------------------------|
| `defaultAuthenticationPlugin`     | `AuthenticationOAuth2Client`   |
| `authenticationPlugins`           | `AuthenticationOAuth2Client`   |
| `useSSL`                          | `true`                         |
| `requireSSL`                      | `true`                         |
| `verifyServerCertificate`         | `false`                        |
| `allowPublicKeyRetrieval`         | `true`                         |

## Flow

1. Click **Test Connection**.
2. DBeaver / the JDBC driver opens your browser to Keycloak.
3. Log in as `peter / iceberg` (or `anna / iceberg`).
4. Keycloak redirects to `http://starrocks.localhost:30080/api/oauth2`,
   the StarRocks FE matches the connection ID in the `state` parameter
   and completes the handshake.
5. The `keycloak_oauth2` security integration (created in
   `starrocks/01_catalog.py`) creates a virtual user keyed by the JWT's
   `preferred_username` claim. The `public` role grants USAGE on the
   `lakekeeper` catalog.
6. Run:

   ```sql
   SET CATALOG lakekeeper;
   SELECT * FROM finance.product;     -- always works
   SELECT * FROM finance.revenue;     -- works as peter, FORBIDDEN as anna
   ```

The JWT is forwarded to Lakekeeper on every catalog/data call, so the
`forbidden` on `finance.revenue` for anna comes from Lakekeeper, not
StarRocks.
