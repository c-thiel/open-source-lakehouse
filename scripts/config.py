"""Shared configuration for workshop scripts."""

LAKEKEEPER_URL = "http://lakekeeper.localhost:30080"
CATALOG_URL = f"{LAKEKEEPER_URL}/catalog"
MANAGEMENT_URL = f"{LAKEKEEPER_URL}/management"

KEYCLOAK_URL = "https://keycloak.localhost:30443"
KEYCLOAK_TOKEN_URL = f"{KEYCLOAK_URL}/realms/iceberg/protocol/openid-connect/token"

# Bootstrap client — used for admin operations
ADMIN_CLIENT_ID = "lakehouse-admin"
ADMIN_CLIENT_SECRET = "3VQlLbU6FbtxmIlKPBIXxQnt2KMfGp4G"

# OPA Bridge — needs server operator to query authorization data
OPA_BRIDGE_CLIENT_ID = "opa-bridge"
OPA_BRIDGE_CLIENT_SECRET = "RCUlsviRgBaf8BtHvjSe1BGfmRoRS5KH"

# Technical clients
TRINO_CLIENT_ID = "trino"
TRINO_CLIENT_SECRET = "AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ"
SPARK_CLIENT_ID = "spark"
SPARK_CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

# Human users (from Keycloak realm)
PETER_USER_ID = "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"

# MinIO S3
S3_ENDPOINT = "http://s3.localhost:30080"
S3_BUCKET = "examples"
S3_ACCESS_KEY = "minio-root-user"
S3_SECRET_KEY = "minio-root-password"
S3_REGION = "us-east-1"
