"""Read-only B2 access for the QC review page's page images. Mirrors
scripts/extract_with_llm.py's load_b2_accounts()/b2_client() (same env var
names, same "account 1 required, account 2 optional" shape) since
pages.b2_account records exactly which of the pipeline's B2 accounts each
page's image was uploaded to -- but never writes, and never downloads
image bytes itself: generate_presigned_url() below hands the browser a
short-lived signed URL straight to B2/S3, so image bytes never pass
through this Vercel function at all (keeps response times and payload
size down, and avoids needing B2 credentials anywhere near the browser).
"""
import os

import boto3


def load_b2_accounts():
    def _account(suffix):
        endpoint = os.environ.get(f"B2_ENDPOINT{suffix}", "")
        key_id = os.environ.get(f"B2_KEY_ID{suffix}", "")
        app_key = os.environ.get(f"B2_APPLICATION_KEY{suffix}", "")
        if not (endpoint and key_id and app_key):
            return None
        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"https://{endpoint}"
        return {"endpoint": endpoint, "key_id": key_id, "app_key": app_key}

    accounts = {}
    for account_id, suffix in (("1", ""), ("2", "_2")):
        account = _account(suffix)
        if account is not None:
            accounts[account_id] = account
    return accounts


def b2_client(account):
    return boto3.client(
        "s3",
        endpoint_url=account["endpoint"],
        aws_access_key_id=account["key_id"],
        aws_secret_access_key=account["app_key"],
    )


def presigned_image_url(b2_accounts, b2_account, b2_bucket, image_key, expires_in=300):
    """Return a short-lived signed GET URL for a page's image, or None if
    b2_account isn't configured in this deployment. Purely a local
    signature computation (no network call), so a stale/misconfigured
    account just yields None rather than raising -- the QC page shows a
    "image unavailable" placeholder instead of a hard error."""
    account = b2_accounts.get(b2_account)
    if account is None:
        return None
    client = b2_client(account)
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": b2_bucket, "Key": image_key},
        ExpiresIn=expires_in,
    )
