"""Deploy examples/batch.py to real AWS on Fargate Spot, run its paths, tear everything down.

Needs credentials plus the network the jobs run in, before pytest imports the example:

    SFNKIT_SUBNETS=subnet-a,subnet-b SFNKIT_SECURITY_GROUPS=sg-x uv run pytest -m e2e tests/test_e2e_batch.py -s

Uses a random stack name, always destroys in a ``finally``, then checks the stacks, bucket, repository,
log groups, compute environment and job queue are gone.
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

    from examples import batch as ex
    from sfnkit.compile.app import build_app
    from sfnkit.deploy.deployer import deploy, destroy, stack_outputs

    if ex.queue.subnets == ("subnet-00000000",):
        pytest.skip("set SFNKIT_SUBNETS and SFNKIT_SECURITY_GROUPS for the Batch e2e")
    stack = f"sfnkit-e2e-{secrets.token_hex(3)}"
    app = build_app(ex.batchy, project_dir=ROOT)
    session = boto3.Session(region_name=REGION)
    cfn, batch = session.client("cloudformation"), session.client("batch")
    logs = session.client("logs")
    # Batch makes its default log group on first use in an account, even though no job logs to it.
    had_batch_logs = bool(logs.describe_log_groups(logGroupNamePrefix="/aws/batch/job")["logGroups"])
    assets: dict = {}
    batch_ids: dict[str, list[str]] = {}
    try:
        res = deploy(app, stack_name=stack, session=session)
        assets = stack_outputs(cfn, f"{stack}-assets")
        for r in cfn.describe_stack_resources(StackName=stack)["StackResources"]:
            batch_ids.setdefault(r["ResourceType"], []).append(r["PhysicalResourceId"])
        yield app, res, session, batch_ids
    finally:
        if not assets:
            try:
                assets = stack_outputs(cfn, f"{stack}-assets")
            except ClientError:
                assets = {}
        destroy(app, stack_name=stack, session=session)
        for name in (stack, f"{stack}-assets"):
            with pytest.raises(ClientError):
                cfn.describe_stacks(StackName=name)
        if assets.get("BucketName"):
            with pytest.raises(ClientError):
                session.client("s3").head_bucket(Bucket=assets["BucketName"])
        if assets.get("RepositoryUri"):
            with pytest.raises(ClientError):
                session.client("ecr").describe_repositories(
                    repositoryNames=[assets["RepositoryUri"].split("/", 1)[1]]
                )
        if not had_batch_logs:
            try:
                logs.delete_log_group(logGroupName="/aws/batch/job")
            except logs.exceptions.ResourceNotFoundException:
                pass
        left = [
            g["logGroupName"]
            for page in logs.get_paginator("describe_log_groups").paginate(logGroupNamePrefix=stack)
            for g in page["logGroups"]
        ]
        assert not left, f"log groups left behind: {left}"
        envs = batch_ids.get("AWS::Batch::ComputeEnvironment", [])
        if envs:
            alive = [
                e
                for e in batch.describe_compute_environments(computeEnvironments=envs)["computeEnvironments"]
                if e["status"] != "DELETED"
            ]
            assert not alive, f"compute environments left behind: {alive}"
        queues = batch_ids.get("AWS::Batch::JobQueue", [])
        if queues:
            alive = [
                q
                for q in batch.describe_job_queues(jobQueues=queues)["jobQueues"]
                if q["status"] != "DELETED"
            ]
            assert not alive, f"job queues left behind: {alive}"
        defs = batch.describe_job_definitions(
            jobDefinitions=batch_ids.get("AWS::Batch::JobDefinition", ["none"])
        )
        active = [
            d for d in defs["jobDefinitions"] if d["status"] == "ACTIVE"
        ]  # deregistered ones stay INACTIVE
        assert not active, f"job definitions still active: {[d['jobDefinitionArn'] for d in active]}"


def _run_all(res, session, inputs: dict[str, dict]) -> dict:
    from sfnkit.deploy.client import ExecutionFailed, result, start, wait

    arns = {k: start(res.state_machine_arn, v, session=session) for k, v in inputs.items()}
    out = {}
    for k, arn in arns.items():
        desc = wait(arn, poll=5, timeout=1800, session=session)
        try:
            out[k] = ("ok", result(desc, session=session), desc)
        except ExecutionFailed as e:
            out[k] = ("failed", e, desc)
    return out


def test_batch_paths(deployed):
    app, res, session, batch_ids = deployed
    runs = _run_all(
        res,
        session,
        {
            "big": {"n": 100, "x": 1, "bad": [5]},
            "over": {"n": 10, "bad": [1, 2]},
            "empty": {"n": 0, "x": -1},
            "fails": {"n": 20, "fail_at": 13},
        },
    )
    for k, (status, val, _desc) in runs.items():
        print(k, status, json.dumps(val if status == "ok" else str(val), default=str)[:400])

    status, big, _ = runs["big"]
    assert status == "ok", big
    assert big["summary"] == {
        "n": 100,
        "ordered": True,
        "jobs": 100,  # one array child per item, each with its own AWS_BATCH_JOB_ID
        "plus_ok": True,  # the second array read the first array's results, 10 items per child
        "extra": [49, 64],  # the literal list went through S3
        "total": sum(i * i + 1 for i in range(100)),  # a single job read 100 array results
        "skipped": [5],  # a tolerated failure: recorded, read back as None
    }
    assert big["checked"] == 10
    assert big["size"] == "big"  # length() of array results in a condition

    status, empty, _ = runs["empty"]
    assert status == "ok", empty
    assert empty["summary"]["n"] == 0 and empty["summary"]["total"] == 0  # empty arrays skip the job
    assert empty["checked"] == "value-error"  # a Batch job's ValueError, caught by name
    assert empty["size"] == "small"

    status, over, _ = runs["over"]
    assert status == "failed" and over.error == "ToleratedFailureExceeded", (
        over
    )  # two bad items, budget of one

    status, fails, desc = runs["fails"]
    assert status == "failed" and fails.error == "LookupError", fails  # an array child's error, by name
    assert "item 13 failed on purpose" in json.dumps(fails.cause, default=str)

    # how the failed array ended, for the record
    batch = session.client("batch")
    name = desc["executionArn"].rsplit(":", 1)[1]
    queue = batch_ids["AWS::Batch::JobQueue"][0]
    jobs = batch.list_jobs(jobQueue=queue, filters=[{"name": "JOB_NAME", "values": [f"{name}*"]}])[
        "jobSummaryList"
    ]
    for j in jobs:
        print("fails job", j["jobName"], j["status"], j.get("statusReason", ""), j.get("arrayProperties", {}))
