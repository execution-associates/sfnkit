"""Deploy examples/pipeline.py to real AWS, run every path, tear everything down.

Run with credentials in the environment: ``uv run pytest -m e2e -s``. Uses a random stack prefix and
always destroys in a ``finally``, then checks nothing tagged for this stack is left.
"""

from __future__ import annotations

import json
import os
import secrets
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

REGION = os.environ.get("SFNKIT_E2E_REGION", "us-west-2")
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def deployed():
    import boto3
    from botocore.exceptions import ClientError

    from examples.pipeline import pipeline
    from sfnkit.compile.app import build_app
    from sfnkit.deploy.deployer import deploy, destroy, stack_outputs

    stack = f"sfnkit-e2e-{secrets.token_hex(3)}"
    app = build_app(pipeline, project_dir=ROOT)
    session = boto3.Session(region_name=REGION)
    cfn = session.client("cloudformation")
    assets = {}
    try:
        res = deploy(app, stack_name=stack, session=session)
        assets = stack_outputs(cfn, f"{stack}-assets")
        session.client("s3").put_object(Bucket=res.bucket, Key="fixtures/data.json", Body=b'{"hello": "world"}')
        yield app, res, session
    finally:
        if not assets:
            try:
                assets = stack_outputs(cfn, f"{stack}-assets")
            except ClientError:
                assets = {}
        destroy(app, stack_name=stack, session=session)
        # nothing left behind
        for name in (stack, f"{stack}-assets"):
            with pytest.raises(ClientError):
                cfn.describe_stacks(StackName=name)
        if assets.get("BucketName"):
            with pytest.raises(ClientError):
                session.client("s3").head_bucket(Bucket=assets["BucketName"])
        if assets.get("RepositoryUri"):
            repo = assets["RepositoryUri"].split("/", 1)[1]
            with pytest.raises(ClientError):
                session.client("ecr").describe_repositories(repositoryNames=[repo])
        logs = session.client("logs")
        left = [
            g["logGroupName"]
            for page in logs.get_paginator("describe_log_groups").paginate(logGroupNamePrefix=stack)
            for g in page["logGroups"]
        ]
        assert not left, f"log groups left behind: {left}"


def _run_all(res, session, inputs: dict[str, dict]) -> dict:
    from sfnkit.deploy.client import ExecutionFailed, result, start, wait

    arns = {k: start(res.state_machine_arn, v, session=session) for k, v in inputs.items()}
    out = {}
    for k, arn in arns.items():
        desc = wait(arn, poll=3, timeout=900, session=session)
        try:
            out[k] = ("ok", result(desc, session=session), desc)
        except ExecutionFailed as e:
            out[k] = ("failed", e, desc)
    return out


def test_every_path(deployed):
    app, res, session = deployed
    from examples.pipeline import pipeline

    runs = _run_all(
        res,
        session,
        {
            "gold": {"n": 50},
            "review": {"n": 5, "tier": "review", "value": 0.9, "charge_mode": "declined"},
            "silver": {"n": 3, "tier": "silver", "charge_mode": "boom", "strict_mode": "value"},
            "low": {"n": 2, "tier": "low"},
            "unhandled": {"n": 2, "strict_mode": "strict"},
        },
    )
    for k, (status, val, _desc) in runs.items():
        print(k, status, json.dumps(val if status == "ok" else str(val), default=str)[:300])

    status, gold, desc = runs["gold"]
    assert status == "ok", gold
    assert gold["receipt"] == {"receipt": "r-published"}
    assert gold["order"] == {"order": "published", "tier": "gold"}
    assert gold["note"] == "receipt ok"
    assert gold["checked"] == "fine"
    assert gold["size"] == {"items": 2000, "bytes": 2000 * 51}
    assert gold["summary"] == {"total": 50, "size": "many"}
    # IAM: inherited base set, base set + extra, own policy only
    assert gold["fixture"] == {"read": '{"hello": "world"}'}
    assert gold["fixture_plus"]["read"] == '{"hello": "world"}'
    assert gold["fixture_plus"]["listed"] == ["fixtures/data.json"]
    assert gold["fixture_own"] == {"denied": "AccessDenied"}
    # Distributed Map: ordered, both branches of the inner condition taken
    assert [t["id"] for t in gold["tagged"]] == list(range(50))
    assert {t["label"] for t in gold["tagged"]} == {"first-half", "second-half"}
    assert gold["tagged"][0]["words"] == "three"
    assert len(gold["described"]) == 50
    # the oversized output really went through S3
    big = next(t for t in pipeline.graph().tasks() if t.step.name == "big_output")
    name = desc["executionArn"].rsplit(":", 1)[1]
    keys = session.client("s3").list_objects_v2(Bucket=res.bucket, Prefix=f"runs/{name}/{big.id}/")
    assert keys.get("KeyCount", 0) >= 1

    status, review, _ = runs["review"]
    assert status == "ok", review
    assert review["order"] == {"order": "queued", "tier": "review"}
    assert review["receipt"] == {"refunded": "queued", "error_type": "PaymentDeclined"}
    assert review["summary"] == {"total": 5, "size": "many"}

    status, silver, _ = runs["silver"]
    assert status == "ok", silver
    assert silver["order"]["order"] == "published"
    assert silver["receipt"] is None
    assert silver["note"] == {"notified": {"msg": "no receipt"}}
    assert silver["checked"] == "value-error"
    assert silver["summary"] == {"total": 3, "size": "few"}

    status, low, _ = runs["low"]
    assert status == "failed" and low.error == "LowScore"

    status, unhandled, _ = runs["unhandled"]
    assert status == "failed" and unhandled.error == "LookupError", unhandled
