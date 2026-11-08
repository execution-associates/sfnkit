"""CDK app for examples/cohort.py: the queues, both workflows and a schedule, in one stack.

    SFNKIT_VPC=vpc-... SFNKIT_SUBNETS=subnet-a,subnet-b SFNKIT_AZS=us-west-2a,us-west-2b \\
    SFNKIT_SECURITY_GROUPS=sg-... cdk deploy --app "python -m examples.cohort_cdk"

The subnets need a route to ECR, S3 and Step Functions; public subnets work (Fargate tasks get a public
IP, and EC2 instances one from the subnet).
"""

from __future__ import annotations

import os
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ec2 as ec2

from examples.cohort import cohort, ticker
from sfnkit.cdk import SCHEDULED_TIME, Ec2Queue, SfnkitWorkflow

ROOT = Path(__file__).resolve().parents[1]


def _list(name: str) -> list[str]:
    return [x for x in os.environ.get(name, "").split(",") if x]


def build(app: cdk.App, name: str, *, synthesizer: cdk.IStackSynthesizer | None = None) -> cdk.Stack:
    stack = cdk.Stack(app, name, synthesizer=synthesizer)
    subnets, azs = _list("SFNKIT_SUBNETS"), _list("SFNKIT_AZS")
    vpc = ec2.Vpc.from_vpc_attributes(
        stack, "Vpc", vpc_id=os.environ["SFNKIT_VPC"], availability_zones=azs, public_subnet_ids=subnets
    )
    groups = [
        ec2.SecurityGroup.from_security_group_id(stack, f"Sg{i}", sg)
        for i, sg in enumerate(_list("SFNKIT_SECURITY_GROUPS"))
    ]
    where = ec2.SubnetSelection(subnets=vpc.public_subnets)

    # Plain CDK Batch constructs work as queues...
    fargate_env = batch.FargateComputeEnvironment(
        stack, "FargateEnv", vpc=vpc, vpc_subnets=where, security_groups=groups, spot=True, maxv_cpus=16
    )
    fargate = batch.JobQueue(
        stack, "Fargate", compute_environments=[batch.OrderedComputeEnvironment(compute_environment=fargate_env, order=1)]
    )
    # ...and Ec2Queue is a shortcut for EC2 with a scratch disk (or GPUs).
    scratch = Ec2Queue(
        stack,
        "Scratch",
        vpc=vpc,
        vpc_subnets=where,
        security_groups=groups,
        spot=False,
        scratch_gib=100,
        instance_types=[ec2.InstanceType("c6i.large")],
        max_vcpus=4,
    )

    wf = SfnkitWorkflow(
        stack,
        "Cohort",
        workflow=cohort,
        project_dir=ROOT,
        queues={"fargate": fargate, "scratch": scratch},
        removal_policy=cdk.RemovalPolicy.DESTROY,
    )
    tick = SfnkitWorkflow(stack, "Ticker", workflow=ticker, project_dir=ROOT, removal_policy=cdk.RemovalPolicy.DESTROY)
    tick.add_schedule("EveryMinute", os.environ.get("SFNKIT_SCHEDULE", "rate(1 hour)"), input={"when": SCHEDULED_TIME})

    for key, w in (("Cohort", wf), ("Ticker", tick)):
        cdk.CfnOutput(stack, f"{key}StateMachine", value=w.state_machine_arn)
        cdk.CfnOutput(stack, f"{key}Bucket", value=w.bucket.bucket_name)
    return stack


def main() -> None:
    app = cdk.App()
    synthesizer = None
    if os.environ.get("SFNKIT_ASSETS_BUCKET"):
        # Assets go to a bucket and repository of our choosing instead of the CDK bootstrap's.
        synthesizer = cdk.CliCredentialsStackSynthesizer(
            file_assets_bucket_name=os.environ["SFNKIT_ASSETS_BUCKET"],
            image_assets_repository_name=os.environ["SFNKIT_ASSETS_REPO"],
            bucket_prefix="",
        )
    stack = build(app, os.environ.get("SFNKIT_STACK", "sfnkit-cohort"), synthesizer=synthesizer)
    tag = os.environ.get("SFNKIT_E2E_TAG")
    if tag:
        cdk.Tags.of(stack).add("sfnkit:e2e", tag)
    app.synth()


if __name__ == "__main__":
    main()
