"""Deploy examples/cohort_cdk.py with `cdk deploy`, run both workflows, then tear everything down.

Covers what only real AWS can show: CDK-built images that Lambda accepts, custom bases (a conda env
with samtools, Debian without Python, a Lambda function on python:3.13-slim), named queues bound to
CDK's Fargate queue and an Ec2Queue with a 100 GiB scratch disk, an input bigger than Batch's request
limit, and an EventBridge schedule that passes its scheduled time.

    SFNKIT_VPC=vpc-... SFNKIT_SUBNETS=subnet-a,subnet-b SFNKIT_SECURITY_GROUPS=sg-x \\
        uv run pytest -m e2e tests/test_e2e_cdk.py -s

Assets go to a bucket and repository the test creates (not the account's CDK bootstrap), and every
resource is removed in a ``finally``: the stack, the asset bucket and repository, and the Batch/ECS
service-linked roles and Batch's default log group if the run created them.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

REGION = os.environ.get("SFNKIT_E2E_REGION", "us-west-2")
ROOT = Path(__file__).resolve().parents[1]
BATCH_LOGS = "/aws/batch/job"
LINKED_ROLES = {
    "AWSServiceRoleForBatch": "batch.amazonaws.com",
    "AWSServiceRoleForECS": "ecs.amazonaws.com",
}


def _cdk(args: list[str], env: dict) -> None:
    out = ROOT / ".sfnkit" / "cdk.out"
    cmd = ["cdk", *args, "--app", f"{sys.executable} -m examples.cohort_cdk", "--output", str(out)]
    proc = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, timeout=3600)
    log = ROOT / ".sfnkit" / f"cdk-{args[0]}.log"
    log.write_text(proc.stdout + "\n" + proc.stderr)
    assert proc.returncode == 0, f"cdk {args[0]} failed, see {log}:\n{proc.stderr[-4000:]}"


def _empty(s3, bucket: str) -> None:
    from sfnkit.deploy.deployer import empty_bucket

    try:
        empty_bucket(s3, bucket)
    except s3.exceptions.NoSuchBucket:
        pass


def _role_exists(iam, name: str) -> bool:
    try:
        iam.get_role(RoleName=name)
        return True
    except iam.exceptions.NoSuchEntityException:
        return False


@pytest.fixture(scope="module")
def deployed():
    import boto3

    for var in ("SFNKIT_VPC", "SFNKIT_SUBNETS", "SFNKIT_SECURITY_GROUPS"):
        if not os.environ.get(var):
            pytest.skip("set SFNKIT_VPC, SFNKIT_SUBNETS and SFNKIT_SECURITY_GROUPS for the CDK e2e")
    if not shutil.which("cdk"):
        pytest.skip("needs the cdk CLI")
    session = boto3.Session(region_name=REGION)
    account = session.client("sts").get_caller_identity()["Account"]
    run = secrets.token_hex(3)
    stack = f"sfnkit-e2e-cdk-{run}"
    assets_bucket, repo = f"{stack}-assets-{account}", stack
    tags = [{"Key": "sfnkit:e2e", "Value": run}, {"Key": "sfnkit:managed", "Value": "true"}]
    s3, ecr, cfn, iam = (session.client(n) for n in ("s3", "ecr", "cloudformation", "iam"))
    had_roles = {name for name in LINKED_ROLES if _role_exists(iam, name)}
    groups = session.client("logs").describe_log_groups(logGroupNamePrefix=BATCH_LOGS)["logGroups"]
    had_batch_logs = any(g["logGroupName"] == BATCH_LOGS for g in groups)
    subnets = os.environ["SFNKIT_SUBNETS"].split(",")
    azs = [s["AvailabilityZone"] for s in session.client("ec2").describe_subnets(SubnetIds=subnets)["Subnets"]]
    env = {
        **os.environ,
        "SFNKIT_STACK": stack,
        "SFNKIT_AZS": ",".join(azs),
        "SFNKIT_ASSETS_BUCKET": assets_bucket,
        "SFNKIT_ASSETS_REPO": repo,
        "SFNKIT_E2E_TAG": run,
        "SFNKIT_SCHEDULE": "rate(1 minute)",
        "CDK_DEFAULT_ACCOUNT": account,
        "CDK_DEFAULT_REGION": REGION,
        "AWS_REGION": REGION,
        "JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION": "1",
        "CDK_DISABLE_VERSION_CHECK": "1",
        "CDK_DISABLE_CLI_TELEMETRY": "true",
    }
    env.pop("BUILDX_NO_DEFAULT_ATTESTATIONS", None)  # the construct must cope without it
    outputs: dict = {}
    try:
        s3.create_bucket(Bucket=assets_bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
        s3.put_bucket_tagging(Bucket=assets_bucket, Tagging={"TagSet": tags})
        s3.put_public_access_block(
            Bucket=assets_bucket,
            PublicAccessBlockConfiguration={k: True for k in (
                "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")},
        )
        ecr.create_repository(repositoryName=repo, tags=tags)
        _cdk(["deploy", "--require-approval", "never", "--progress", "events"], env)
        desc = cfn.describe_stacks(StackName=stack)["Stacks"][0]
        outputs = {o["OutputKey"]: o["OutputValue"] for o in desc.get("Outputs", [])}
        yield session, stack, outputs
    finally:
        for key in ("CohortBucket", "TickerBucket"):
            if outputs.get(key):
                _empty(s3, outputs[key])
        try:
            _cdk(["destroy", "--force"], env)
        finally:
            with pytest.raises(cfn.exceptions.ClientError):
                cfn.describe_stacks(StackName=stack)
            _empty(s3, assets_bucket)
            try:
                s3.delete_bucket(Bucket=assets_bucket)
            except s3.exceptions.NoSuchBucket:
                pass
            try:
                ecr.delete_repository(repositoryName=repo, force=True)
            except ecr.exceptions.RepositoryNotFoundException:
                pass
            problems = []
            for name in sorted(set(LINKED_ROLES) - had_roles):
                if _role_exists(iam, name):
                    iam.delete_service_linked_role(RoleName=name)
                    # The deletion task's status can 404 right after the call, so watch the role itself.
                    for _ in range(60):
                        if not _role_exists(iam, name):
                            break
                        time.sleep(5)
                    else:
                        problems.append(f"{name} still exists")
            logs = session.client("logs")
            if not had_batch_logs:  # Batch makes its default group on first use, even when no job logs to it
                try:
                    logs.delete_log_group(logGroupName=BATCH_LOGS)
                except logs.exceptions.ResourceNotFoundException:
                    pass
            left = [
                g["logGroupName"]
                for page in logs.get_paginator("describe_log_groups").paginate(logGroupNamePrefix=stack)
                for g in page["logGroups"]
            ]
            if left:
                problems.append(f"log groups left behind: {left}")
            assert not problems, problems


def test_cohort_runs_on_custom_images_and_cdk_queues(deployed):
    from sfnkit.deploy import client

    session, stack, outputs = deployed
    manifest = "x" * 20_000  # far over SubmitJob's 30 KiB with the token, and over ECS's 8 KiB overrides
    out = client.run(outputs["CohortStateMachine"], {"n": 3, "manifest": manifest}, session=session, timeout=2400)
    print("cohort", json.dumps(out)[:600])
    assert out["samples"] == 3 and out["reads"] == 1 + 2 + 3
    assert out["samtools"] and out["samtools"][0][0].isdigit()  # samtools from bioconda ran in the conda env
    assert out["manifest_bytes"] == 20_000
    assert out["scratch_gib"] >= 90  # the Ec2Queue's 100 GiB root volume
    assert out["jq"] == 2
    assert "reads: 6" in out["yaml"]  # pyyaml, on the python:3.13-slim Lambda


def test_schedule_starts_the_workflow_with_its_scheduled_time(deployed):
    session, stack, outputs = deployed
    sfn = session.client("stepfunctions")
    arn = outputs["TickerStateMachine"]
    deadline = time.time() + 300
    while time.time() < deadline:
        runs = sfn.list_executions(stateMachineArn=arn, maxResults=5)["executions"]
        done = [r for r in runs if r["status"] == "SUCCEEDED"]
        if done:
            run = sfn.describe_execution(executionArn=done[0]["executionArn"])
            when = json.loads(run["input"])["when"]
            datetime.fromisoformat(when.replace("Z", "+00:00"))  # an ISO timestamp, filled in by Scheduler
            print("ticker", when)
            return
        time.sleep(15)
    raise AssertionError("the schedule didn't start a successful run within 5 minutes")
