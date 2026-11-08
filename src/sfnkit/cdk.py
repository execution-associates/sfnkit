"""sfnkit workflows as CDK constructs (``pip install "sfnkit[cdk]"``).

``SfnkitWorkflow`` puts a workflow into any CDK stack: the same functions, job definitions, roles and
state machines ``sfnkit deploy`` creates, as native CloudFormation resources of that stack. Images are
CDK ``DockerImageAsset``s built from sfnkit's build contexts, so ``cdk deploy`` builds and pushes them
like any other asset. The construct exposes the state machine as an ``IStateMachine``, so anything
that can start a state machine in CDK can trigger the workflow: EventBridge rules, Pipes from SQS, API
Gateway, ``grant_start_execution``.

What stays outside sfnkit is plain CDK too: VPCs, Batch compute environments and queues (``Ec2Queue``
here is a shortcut for the common EC2 case), the sources that trigger a workflow.

Names: state machines and job definitions are named ``<stack name>-<construct id>-<name>``, so several
workflows can share one stack.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import aws_cdk as cdk
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

from sfnkit.build.context import prepare_context
from sfnkit.compile.app import build_app
from sfnkit.compile.cfn import BUCKET_PARAM, main_template
from sfnkit.compute.batch import named_queues
from sfnkit.deploy.deployer import MAX_SM_NAME
from sfnkit.errors import DeployError
from sfnkit.runtime import protocol as P

# EventBridge Scheduler fills this in with the time the run was scheduled for (not when it started), so a
# late or re-driven run still knows which interval it covers.
SCHEDULED_TIME = "<aws.scheduler.scheduled-time>"

NO_ATTESTATIONS = "type=image,attestation-inline=false"
_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_SUB_VAR = re.compile(r"\$\{(?!!)([^}]+)\}")


class Ec2Queue(Construct):
    """A Batch job queue on EC2: for GPUs, more than 16 vCPUs or 120 GiB, or a big scratch disk.

    ``scratch_gib`` sizes each instance's root volume (gp3, encrypted). Containers write to it, so it is
    the job's scratch space; bigger instances run several jobs on the same disk. ``gpu=True`` uses the
    ECS GPU AMI, and G5 instances unless ``instance_types``/``instance_classes`` say otherwise.
    Bind it to steps with ``SfnkitWorkflow(queues={"name": queue})`` and ``BatchQueue("name",
    platform="EC2")``.
    """

    platform = "EC2"

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        vpc: ec2.IVpc,
        spot: bool = True,
        max_vcpus: int = 256,
        scratch_gib: int | None = None,
        gpu: bool = False,
        instance_types: Sequence[ec2.InstanceType] | None = None,
        instance_classes: Sequence[ec2.InstanceClass] | None = None,
        vpc_subnets: ec2.SubnetSelection | None = None,
        security_groups: Sequence[ec2.ISecurityGroup] | None = None,
    ):
        super().__init__(scope, id)
        if scratch_gib is not None and not 30 <= scratch_gib <= 16384:
            raise ValueError("scratch_gib must be 30..16384 (the ECS AMI's root volume is 30 GiB already)")
        if gpu and not instance_types and not instance_classes:
            instance_classes = [ec2.InstanceClass.G5]
        launch_template = None
        if scratch_gib:
            launch_template = ec2.LaunchTemplate(
                self,
                "LaunchTemplate",
                block_devices=[
                    ec2.BlockDevice(
                        device_name="/dev/xvda",  # the ECS AL2023 AMI's root device; Docker's storage lives on it
                        volume=ec2.BlockDeviceVolume.ebs(
                            scratch_gib,
                            volume_type=ec2.EbsDeviceVolumeType.GP3,
                            encrypted=True,
                            delete_on_termination=True,
                        ),
                    )
                ],
            )
        image_type = batch.EcsMachineImageType.ECS_AL2023_NVIDIA if gpu else batch.EcsMachineImageType.ECS_AL2023
        self.compute_environment = batch.ManagedEc2EcsComputeEnvironment(
            self,
            "Environment",
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=list(security_groups) if security_groups else None,
            spot=spot,
            maxv_cpus=max_vcpus,
            instance_types=list(instance_types) if instance_types else None,
            instance_classes=list(instance_classes) if instance_classes else None,
            use_optimal_instance_classes=not (instance_types or instance_classes),
            launch_template=launch_template,
            images=[batch.EcsMachineImage(image_type=image_type)],
        )
        self.job_queue = batch.JobQueue(
            self,
            "Queue",
            compute_environments=[
                batch.OrderedComputeEnvironment(compute_environment=self.compute_environment, order=1)
            ],
        )

    @property
    def job_queue_arn(self) -> str:
        return self.job_queue.job_queue_arn


class SfnkitWorkflow(Construct):
    """A workflow and everything it runs on, inside a CDK stack.

    ``queues`` binds each named ``BatchQueue("name")`` to a CDK job queue (``batch.IJobQueue``, an
    ``Ec2Queue``) or an ARN. ``bucket`` replaces the artifacts bucket the construct creates otherwise;
    run artifacts under ``runs/`` expire after the workflow's ``run_retention_days``. The bucket keeps
    CDK's default of surviving the stack; pass ``removal_policy=cdk.RemovalPolicy.DESTROY`` and
    ``auto_delete_objects=True`` to have it go with the stack.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        workflow,
        project_dir: str | Path | None = None,
        queues: Mapping[str, Any] | None = None,
        bucket: s3.IBucket | None = None,
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.RETAIN,
        auto_delete_objects: bool = False,
    ):
        super().__init__(scope, id)
        if not _NAME.match(id):
            raise ValueError(f"SfnkitWorkflow id {id!r} names AWS resources: use letters, digits, - and _")
        self.sfnkit_app = app = build_app(workflow, project_dir=Path(project_dir) if project_dir else None)
        self.workflow = workflow
        stack = cdk.Stack.of(self)
        if not cdk.Token.is_unresolved(stack.stack_name):
            for sm in app.state_machines:
                full = f"{stack.stack_name}-{id}-{sm.name}"
                if len(full) > MAX_SM_NAME:
                    raise ValueError(
                        f"state machine name {full!r} is {len(full)} characters (Step Functions allows "
                        f"{MAX_SM_NAME}); use a shorter stack name or construct id"
                    )

        if bucket is None:
            bucket = s3.Bucket(
                self,
                "Artifacts",
                encryption=s3.BucketEncryption.S3_MANAGED,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
                enforce_ssl=True,
                lifecycle_rules=[
                    s3.LifecycleRule(
                        id="expire-runs",
                        prefix=P.RUNS_PREFIX,
                        expiration=cdk.Duration.days(app.settings.run_retention_days),
                    ),
                    s3.LifecycleRule(
                        id="abort-uploads", abort_incomplete_multipart_upload_after=cdk.Duration.days(1)
                    ),
                ],
                removal_policy=removal_policy,
                auto_delete_objects=auto_delete_objects,
            )
        self.bucket = bucket
        values: dict[str, Any] = {BUCKET_PARAM: bucket.bucket_name}

        self.images: dict[str, ecr_assets.DockerImageAsset] = {}
        for image in app.images:
            context = prepare_context(app, image)
            asset = ecr_assets.DockerImageAsset(
                self,
                f"Image{image.key}",
                directory=str(context.path),
                platform=ecr_assets.Platform.custom(context.platform),
                display_name=f"{id} image {image.key}",
                # Docker 29 and buildx attach provenance attestations by default, which turns the image
                # into an index that Lambda rejects. This keeps it a single manifest (`sfnkit deploy`
                # passes --provenance=false for the same reason).
                outputs=[NO_ATTESTATIONS],
            )
            self.images[image.key] = asset
            values[image.param_name] = asset.image_uri

        values.update(_bind_queues(app, queues or {}))

        template = main_template(app)
        unbound = sorted(set(template.get("Parameters", {})) - set(values))
        if unbound:  # a backend asked for a parameter this construct doesn't know how to fill
            raise DeployError(f"internal: no value for template parameters {unbound}")
        self.resources = _emit(self, template["Resources"], values, prefix=id)

        root = app.root
        self.state_machine_arn: str = self.resources[root.logical_id].ref
        self.state_machine: sfn.IStateMachine = sfn.StateMachine.from_state_machine_arn(
            self, "StateMachine", self.state_machine_arn
        )
        self.state_machine_name = cdk.Fn.join("-", [cdk.Aws.STACK_NAME, id, root.name])
        self.schedules: dict[str, scheduler.CfnSchedule] = {}
        self._scheduler_role: iam.Role | None = None

    def add_schedule(
        self,
        id: str,
        expression: str,
        *,
        input: Mapping[str, Any] | None = None,
        timezone: str | None = None,
        enabled: bool = True,
        description: str | None = None,
        flexible_window_minutes: int | None = None,
    ) -> scheduler.CfnSchedule:
        """Start the workflow on a schedule (EventBridge Scheduler).

        ``expression`` is ``rate(...)``, ``cron(...)`` or a one-time ``at(...)``. ``input`` holds the
        workflow's arguments; put ``SCHEDULED_TIME`` where the run should learn which slot it is for,
        so a backfill or a late run processes the right interval.
        """
        args = dict(input or {})
        self._check_input(args)
        if self._scheduler_role is None:
            self._scheduler_role = iam.Role(
                self,
                "SchedulerRole",
                assumed_by=iam.ServicePrincipal(
                    "scheduler.amazonaws.com",
                    conditions={"StringEquals": {"aws:SourceAccount": cdk.Aws.ACCOUNT_ID}},
                ),
            )
            self.state_machine.grant_start_execution(self._scheduler_role)
        window = (
            scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="FLEXIBLE", maximum_window_in_minutes=flexible_window_minutes
            )
            if flexible_window_minutes
            else scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF")
        )
        sched = scheduler.CfnSchedule(
            self,
            f"Schedule{id}",
            schedule_expression=expression,
            schedule_expression_timezone=timezone,
            state="ENABLED" if enabled else "DISABLED",
            description=description,
            flexible_time_window=window,
            target=scheduler.CfnSchedule.TargetProperty(
                arn=self.state_machine_arn,
                role_arn=self._scheduler_role.role_arn,
                input=json.dumps(args),
            ),
        )
        self.schedules[id] = sched
        return sched

    def _check_input(self, args: dict) -> None:
        sig = self.workflow.signature
        unknown = sorted(set(args) - set(sig.parameters))
        if unknown:
            raise ValueError(f"schedule input for workflow {self.workflow.name}: unknown arguments {unknown}")
        missing = [n for n, p in sig.parameters.items() if p.default is p.empty and n not in args]
        if missing:
            raise ValueError(f"schedule input for workflow {self.workflow.name}: missing arguments {missing}")
        json.dumps(args)  # the input must be JSON, like any execution's


def _queue_arn(name: str, q: Any) -> str:
    if isinstance(q, str):
        return q
    arn = getattr(q, "job_queue_arn", None)  # batch.IJobQueue and Ec2Queue both have it
    if arn is None:
        raise TypeError(f"queues[{name!r}] must be a job queue ARN, a batch.IJobQueue or an Ec2Queue")
    return arn


def _bind_queues(app, queues: Mapping[str, Any]) -> dict[str, str]:
    named = named_queues(app.functions)
    unknown = sorted(set(queues) - set(named))
    if unknown:
        raise ValueError(f"queues: no step runs on a queue named {', '.join(unknown)}")
    missing = sorted(set(named) - set(queues))
    if missing:
        raise ValueError(f"queues: steps run on the named Batch queue(s) {', '.join(missing)}; bind them here")
    out = {}
    for name, q in queues.items():
        platform = getattr(q, "platform", None)
        if platform and platform != named[name].platform:
            raise ValueError(
                f"queue {name!r} is an {platform} queue, but its steps are declared for "
                f"{named[name].platform}: use BatchQueue({name!r}, platform={platform!r})"
            )
        out[named[name].param_id] = _queue_arn(name, q)
    return out


# --------------------------------------------------------------------------- template -> CDK resources


def _refs(v: Any, ids: set[str]) -> set[str]:
    """Logical ids of ``ids`` that ``v`` references (Ref, GetAtt, Sub variables, DependsOn)."""
    out: set[str] = set()
    if isinstance(v, dict):
        if len(v) == 1:
            (k, x), = v.items()
            if k == "Ref" and x in ids:
                return {x}
            if k == "Fn::GetAtt" and x[0] in ids:
                return {x[0]}
            if k == "Fn::Sub" and isinstance(x, str):
                return {m.split(".")[0] for m in _SUB_VAR.findall(x)} & ids
        for x in v.values():
            out |= _refs(x, ids)
    elif isinstance(v, list):
        for x in v:
            out |= _refs(x, ids)
    return out


def _order(resources: dict[str, dict]) -> list[str]:
    ids = set(resources)
    deps = {lid: _refs(r.get("Properties", {}), ids) | set(r.get("DependsOn", [])) for lid, r in resources.items()}
    done: list[str] = []
    seen: set[str] = set()

    def visit(lid: str, path: tuple[str, ...]) -> None:
        if lid in seen:
            return
        if lid in path:
            raise DeployError(f"internal: reference cycle {' -> '.join(path + (lid,))}")
        for d in sorted(deps[lid]):
            visit(d, path + (lid,))
        seen.add(lid)
        done.append(lid)

    for lid in sorted(resources):
        visit(lid, ())
    return done


def _emit(scope: Construct, resources: dict[str, dict], values: dict[str, Any], prefix: str) -> dict[str, cdk.CfnResource]:
    """Recreate sfnkit's resources as ``CfnResource``s whose references are CDK tokens."""
    made: dict[str, cdk.CfnResource] = {}

    def sub(body: str) -> Any:
        variables: dict[str, str] = {}

        def repl(m: re.Match) -> str:
            ref = m.group(1)
            if ref == "AWS::StackName":
                return "${AWS::StackName}-" + prefix  # names are unique per construct, not per stack
            if ref.startswith("AWS::"):
                return m.group(0)
            name, _, attr = ref.partition(".")
            if name in values and not attr:
                val = values[name]
            elif name in made:
                val = made[name].get_att(attr).to_string() if attr else made[name].ref
            else:
                raise DeployError(f"internal: unknown reference ${{{ref}}}")
            var = f"v{len(variables)}"
            variables[var] = val
            return "${" + var + "}"

        out = _SUB_VAR.sub(repl, body)
        return cdk.Fn.sub(out, variables) if variables else cdk.Fn.sub(out)

    def tr(v: Any) -> Any:
        if isinstance(v, dict):
            if len(v) == 1:
                (k, x), = v.items()
                if k == "Ref":
                    if x in values:
                        return values[x]
                    if x in made:
                        return made[x].ref
                    if x.startswith("AWS::"):
                        return v
                    raise DeployError(f"internal: unknown Ref {x}")
                if k == "Fn::GetAtt":
                    return made[x[0]].get_att(x[1])
                if k == "Fn::Sub" and isinstance(x, str):
                    return sub(x)
            return {k: tr(x) for k, x in v.items()}
        if isinstance(v, list):
            return [tr(x) for x in v]
        return v

    for lid in _order(resources):
        res = resources[lid]
        r = cdk.CfnResource(scope, lid, type=res["Type"], properties=tr(res.get("Properties", {})))
        for dep in res.get("DependsOn", []):
            r.add_dependency(made[dep])
        made[lid] = r
    return made


__all__ = ["SCHEDULED_TIME", "Ec2Queue", "SfnkitWorkflow"]
