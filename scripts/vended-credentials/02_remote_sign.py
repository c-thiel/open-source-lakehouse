"""Remote-signing demo: Lakekeeper signs S3 requests on the client's behalf.

The Iceberg REST spec defines an *S3 remote signing* extension: instead of
handing out S3 credentials, the catalog exposes a `/v1/aws/s3/sign` endpoint.
The client builds an unsigned S3 request locally, posts the request metadata
(method/uri/headers) to the signer, and gets back signed headers it can attach
before forwarding the request to S3 itself.

This is exactly what PyIceberg's `S3V4RestSigner` does internally
(see `pyiceberg.io.fsspec.S3V4RestSigner`). Here we reproduce it by hand for
*one* GetObject request against a data file from finance.product, so the
mechanics are visible:

  1. loadTable → get a data file's S3 URI + the signer endpoint from `config`.
  2. Build an unsigned AWSRequest for GetObject(file).
  3. POST {method, region, uri, headers} to Lakekeeper's signer.
  4. Take signed headers + (possibly rewritten) URI from the response,
     fire the actual GET to S3, print the first bytes of the Parquet file.
"""

import io

import httpx
import urllib3
from botocore.awsrequest import AWSRequest
from lib.auth import get_token
from lib.config import (
    CATALOG_URL,
    NAMESPACE_NAME,
    S3_REGION,
    TABLE_PRODUCT,
    WAREHOUSE_NAME,
    get_sp,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main() -> None:
    sp_id, sp_secret = get_sp()
    token = get_token(sp_id, sp_secret)
    auth = {"Authorization": f"Bearer {token}"}

    # --- 1. Discover prefix and load the table -----------------------------
    cfg = httpx.get(
        f"{CATALOG_URL}/v1/config",
        params={"warehouse": WAREHOUSE_NAME},
        headers=auth,
        verify=False,
    )
    cfg.raise_for_status()
    prefix = cfg.json()["defaults"]["prefix"]

    load_resp = httpx.get(
        f"{CATALOG_URL}/v1/{prefix}/namespaces/{NAMESPACE_NAME}/tables/{TABLE_PRODUCT}",
        headers={**auth, "X-Iceberg-Access-Delegation": "remote-signing"},
        verify=False,
    )
    load_resp.raise_for_status()
    table = load_resp.json()
    table_config: dict[str, str] = table.get("config", {})

    # The signer URI may be returned in the config; otherwise default to the
    # catalog URI itself (Iceberg REST spec behavior).
    signer_uri = table_config.get("s3.signer.uri", CATALOG_URL).rstrip("/")
    signer_endpoint = table_config.get("s3.signer.endpoint", "v1/aws/s3/sign")
    print(f"\nSigner endpoint: {signer_uri}/{signer_endpoint}")

    # --- 2. Pick a snapshot's manifest-list as the target object ----------
    # The loadTable response embeds the table metadata, including the snapshot
    # list — each snapshot has a `manifest-list` S3 location. That's a real
    # data-plane object owned by the table, and we get it without doing any
    # extra S3 reads ourselves.
    snapshots = table["metadata"].get("snapshots") or []
    if not snapshots:
        raise SystemExit("Table has no snapshots — run 00_setup/03_data.py first.")
    target_object = snapshots[-1]["manifest-list"]
    print(f"Target object: {target_object}")

    # --- 3. Build an unsigned AWSRequest for GetObject --------------------
    # Translate s3://bucket/key → endpoint URL using the workshop S3 endpoint.
    assert target_object.startswith("s3://")
    bucket, _, key = target_object[len("s3://") :].partition("/")
    s3_endpoint = table_config.get("s3.endpoint", "http://s3.localtest.me:30080")
    object_url = f"{s3_endpoint.rstrip('/')}/{bucket}/{key}"

    aws_request = AWSRequest(
        method="GET", url=object_url, headers={"host": httpx.URL(object_url).host}
    )

    # --- 4. POST request metadata to Lakekeeper's signer ------------------
    sign_body = {
        "method": aws_request.method,
        "region": table_config.get("client.region", S3_REGION),
        "uri": aws_request.url,
        "headers": {k: [v] for k, v in aws_request.headers.items()},
    }
    print(f"\nPOST {signer_uri}/{signer_endpoint}")
    sign_resp = httpx.post(
        f"{signer_uri}/{signer_endpoint}",
        headers=auth,
        json=sign_body,
        verify=False,
    )
    sign_resp.raise_for_status()
    signed = sign_resp.json()

    print("\n--- signed response ---")
    print(f"uri:     {signed['uri']}")
    print("headers:")
    for k, v in signed["headers"].items():
        print(f"  {k}: {', '.join(v)}")

    # --- 5. Forward the actual request to S3 ------------------------------
    final_headers = {k: ", ".join(v) for k, v in signed["headers"].items()}
    s3_resp = httpx.get(signed["uri"], headers=final_headers)
    s3_resp.raise_for_status()
    print(f"\nS3 GET {s3_resp.status_code} — {len(s3_resp.content)} bytes")

    # Manifest-list files are Avro — decode and print the records so we can
    # see we actually got real bytes, not an error response.
    from fastavro import reader

    print("\n--- manifest-list records ---")
    for record in reader(io.BytesIO(s3_resp.content)):
        print(record)


if __name__ == "__main__":
    main()
