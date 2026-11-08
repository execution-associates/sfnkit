"""sfnkit.cdk: workflows synthesized into CDK stacks (no Docker or AWS needed)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:  # for examples.checkout
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION", "1")
cdk = pytest.importorskip("aws_cdk")

import cdk_flows  # noqa: E402
from aws_cdk import aws_batch as batch  # noqa: E402
from aws_cdk import aws_ec2 as ec2  # noqa: E402
from aws_cdk import aws_iam as iam  # noqa: E402

from examples.checkout import checkout  # noqa: E402
from sfnkit.cdk import NO_ATTESTATIONS, SCHEDULED_TIME, Ec2Queue, SfnkitWorkflow, _order  # noqa: E402

ENV = cdk.Environment(account="123456789012", region="us-west-2")


def _stack(tmp_path, name="Demo"):
    app = cdk.App(outdir=str(tmp_path / "cdk.out"))
    return app, cdk.Stack(app, name, env=ENV)


def _vpc(stack):
    return ec2.Vpc.from_vpc_attributes(
        stack,
        "Vpc",
        vpc_id="vpc-0123456789abcdef0",
        availability_zones=["us-west-2a"],
        public_subnet_ids=["subnet-0123456789abcdef0"],
    )


def _fargate_queue(stack, vpc):
    env = batch.FargateComputeEnvironment(stack, "CpuEnv", vpc=vpc, spot=True)
    return batch.JobQueue(
        stack, "Cpu", compute_environments=[batch.OrderedComputeEnvironment(compute_environment=env, order=1)]
    )


def _template(app, stack) -> dict:
    return app.synth().get_stack_by_name(stack.stack_name).template


def _lint(template: dict) -> list[str]:
    from cfnlint.api import ManualArgs, lint

    matches = lint(json.dumps(template), config=ManualArgs(regions=["us-west-2"]))
    return [f"{m.rule.id} {m.message}" for m in matches if m.rule.id.startswith("E")]


def _of(template: dict, kind: str) -> dict[str, dict]:
    return {k: r for k, r in template["Resources"].items() if r["Type"] == kind}


@pytest.fixture
def cohort(tmp_path):
    app, stack = _stack(tmp_path)
    vpc = _vpc(stack)
    gpu = Ec2Queue(stack, "Gpu", vpc=vpc, gpu=True, scratch_gib=500)
    wf = SfnkitWorkflow(
        stack, "Cohort", workflow=cdk_flows.cohort, project_dir=ROOT, queues={"gpu": gpu, "cpu": _fargate_queue(stack, vpc)}
    )
    return app, stack, wf


def test_workflow_synthesizes_and_lints_clean(cohort):
    app, stack, wf = cohort
    tpl = _template(app, stack)
    assert _lint(tpl) == []
    assert len(_of(tpl, "AWS::StepFunctions::StateMachine")) == 1
    assert len(_of(tpl, "AWS::Batch::JobDefinition")) == 2
    assert len(_of(tpl, "AWS::Lambda::Function")) == 1


def test_names_are_scoped_by_construct_id(cohort):
    app, stack, wf = cohort
    tpl = _template(app, stack)
    (sm,) = _of(tpl, "AWS::StepFunctions::StateMachine").values()
    assert sm["Properties"]["StateMachineName"] == {"Fn::Sub": "${AWS::StackName}-Cohort-main"}
    names = [r["Properties"]["JobDefinitionName"]["Fn::Sub"] for r in _of(tpl, "AWS::Batch::JobDefinition").values()]
    assert all(n.startswith("${AWS::StackName}-Cohort-Fn") for n in names)
    # the state machine may submit exactly those job definitions
    policy = json.dumps(tpl["Resources"])
    for n in names:
        assert f"job-definition/{n}:*" in policy


def test_named_queues_bind_to_cdk_queues(cohort):
    app, stack, wf = cohort
    tpl = _template(app, stack)
    (sm,) = _of(tpl, "AWS::StepFunctions::StateMachine").values()
    subs = sm["Properties"]["DefinitionSubstitutions"]
    queues = sorted(v["Fn::GetAtt"][0] for k, v in subs.items() if k.endswith("Queue"))
    assert queues[0].startswith("Cpu") and queues[1].startswith("GpuQueue")
    assert "Parameters" not in tpl or not any(p.startswith("Queue") for p in tpl.get("Parameters", {}))


def test_images_are_docker_assets_with_a_single_manifest(cohort):
    app, stack, wf = cohort
    asm = app.synth()
    manifest = json.loads(Path(asm.directory, f"{stack.artifact_id}.assets.json").read_text())
    images = manifest["dockerImages"].values()
    assert len(wf.images) == len(images) == 3  # Lambda base, Batch on the Lambda base, Batch on debian
    for img in images:
        assert img["source"]["dockerOutputs"] == [NO_ATTESTATIONS]
        assert img["source"]["platform"] == "linux/amd64"
    dockerfiles = [Path(asm.directory, i["source"]["directory"], "Dockerfile").read_text() for i in images]
    assert any("FROM debian:bookworm-slim" in d for d in dockerfiles)


def test_ec2_queue_scratch_disk_and_gpu_ami(cohort):
    app, stack, wf = cohort
    tpl = _template(app, stack)
    (lt,) = _of(tpl, "AWS::EC2::LaunchTemplate").values()
    (bdm,) = lt["Properties"]["LaunchTemplateData"]["BlockDeviceMappings"]
    assert bdm["DeviceName"] == "/dev/xvda" and bdm["Ebs"]["VolumeSize"] == 500 and bdm["Ebs"]["VolumeType"] == "gp3"
    envs = [r["Properties"] for r in _of(tpl, "AWS::Batch::ComputeEnvironment").values()]
    ec2_env = next(e for e in envs if e["ComputeResources"]["Type"] in ("SPOT", "EC2"))
    assert ec2_env["ComputeResources"]["Ec2Configuration"] == [{"ImageType": "ECS_AL2023_NVIDIA"}]
    assert ec2_env["ComputeResources"]["InstanceTypes"] == ["g5"]


def test_state_machine_handle_grants_and_schedules(tmp_path):
    app, stack = _stack(tmp_path)
    wf = SfnkitWorkflow(stack, "Checkout", workflow=checkout, project_dir=ROOT)
    role = iam.Role(stack, "Caller", assumed_by=iam.AccountRootPrincipal())
    wf.state_machine.grant_start_execution(role)
    wf.add_schedule("Hourly", "rate(1 hour)", input={"key": "orders/" + SCHEDULED_TIME}, timezone="UTC")
    tpl = _template(app, stack)
    assert _lint(tpl) == []
    (sched,) = _of(tpl, "AWS::Scheduler::Schedule").values()
    target = sched["Properties"]["Target"]
    assert json.loads(target["Input"]) == {"key": "orders/<aws.scheduler.scheduled-time>"}
    assert target["Arn"]["Ref"].startswith("CheckoutSmMain")
    grants = json.dumps(_of(tpl, "AWS::IAM::Policy"))
    assert "states:StartExecution" in grants


def test_schedule_input_is_checked_against_the_workflow(tmp_path):
    app, stack = _stack(tmp_path)
    wf = SfnkitWorkflow(stack, "Checkout", workflow=checkout, project_dir=ROOT)
    with pytest.raises(ValueError, match="missing arguments"):
        wf.add_schedule("A", "rate(1 day)", input={})
    with pytest.raises(ValueError, match="unknown arguments"):
        wf.add_schedule("B", "rate(1 day)", input={"key": "k", "nope": 1})


def test_two_workflows_share_a_stack(tmp_path):
    app, stack = _stack(tmp_path)
    SfnkitWorkflow(stack, "A", workflow=checkout, project_dir=ROOT)
    SfnkitWorkflow(stack, "B", workflow=checkout, project_dir=ROOT)
    tpl = _template(app, stack)
    names = sorted(r["Properties"]["StateMachineName"]["Fn::Sub"] for r in _of(tpl, "AWS::StepFunctions::StateMachine").values())
    assert names == [f"${{AWS::StackName}}-{w}-{n}" for w in "AB" for n in ("main", "risk")]
    assert _lint(tpl) == []


def test_queue_binding_errors(tmp_path):
    app, stack = _stack(tmp_path)
    vpc = _vpc(stack)
    cpu = _fargate_queue(stack, vpc)
    with pytest.raises(ValueError, match="gpu; bind them here"):
        SfnkitWorkflow(stack, "A", workflow=cdk_flows.cohort, project_dir=ROOT, queues={"cpu": cpu})
    with pytest.raises(ValueError, match="no step runs on a queue named extra"):
        SfnkitWorkflow(stack, "B", workflow=cdk_flows.cohort, project_dir=ROOT, queues={"cpu": cpu, "gpu": cpu, "extra": cpu})
    # an EC2 queue for steps declared as Fargate
    with pytest.raises(ValueError, match="EC2 queue"):
        SfnkitWorkflow(stack, "C", workflow=cdk_flows.cohort, project_dir=ROOT,
                       queues={"cpu": Ec2Queue(stack, "Wrong", vpc=vpc), "gpu": "arn:aws:batch:us-west-2:1:job-queue/g"})


def test_bad_construct_id_and_long_names(tmp_path):
    app, stack = _stack(tmp_path)
    with pytest.raises(ValueError, match="names AWS resources"):
        SfnkitWorkflow(stack, "has space", workflow=checkout, project_dir=ROOT)
    app2, long = _stack(tmp_path, name="S" * 70)
    with pytest.raises(ValueError, match="Step Functions allows 80"):
        SfnkitWorkflow(long, "Checkout", workflow=checkout, project_dir=ROOT)


def test_resources_are_created_after_what_they_reference():
    resources = {
        "B": {"Type": "X", "Properties": {"A": {"Fn::GetAtt": ["A", "Arn"]}}},
        "A": {"Type": "X", "Properties": {"N": {"Fn::Sub": "${C.Arn}-x"}}},
        "C": {"Type": "X", "Properties": {}},
        "D": {"Type": "X", "DependsOn": ["B"]},
    }
    order = _order(resources)
    assert order.index("C") < order.index("A") < order.index("B") < order.index("D")
