import os

import pytest


@pytest.fixture(autouse=True)
def _no_real_aws(monkeypatch):
    # Every infra test runs against fakes; make sure nothing can reach a real account.
    for k in list(os.environ):
        if k.startswith("AWS_") and k not in ("AWS_DEFAULT_REGION",):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")


@pytest.fixture
def s3():
    from moto import mock_aws

    with mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-west-2")
        client.create_bucket(Bucket="art", CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        yield client
