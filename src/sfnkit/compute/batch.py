"""AWS Batch backend: a step runs as a Batch job, and ``step.map(items)`` runs as one array job.

Transport. The state machine submits the job with ``batch:submitJob.sync`` and passes the envelope and
the task token in the container's environment. The container (``python -m sfnkit.runtime.container``)
finishes the task itself with SendTaskSuccess/SendTaskFailure, so the result arrives as the usual
wrapper and an error keeps its Python class name, exactly as with Lambda. If the container dies before
it can report (out of memory, a lost host on the last retry), the job's own failure ends the task as
``States.TaskFailed``.

Array jobs. Each child reads its items from S3 by ``AWS_BATCH_JOB_ARRAY_INDEX`` and writes one result
file per item. The task completes when the whole array job does. A failing child fails the task with
its exception's name and terminates the rest of the array.

Retries come in two layers. Batch retries infrastructure failures (image pull errors, Spot reclaims,
lost hosts) inside the job, up to ``retries`` attempts. A step's own exceptions, running out of memory
and the attempt timeout are never retried by Batch, since they would fail again. Exceptions go to Step
Functions, where ``Retry(...)`` and ``attempt(...)`` see them by name.

Step Functions can only wait for Batch jobs in STANDARD workflows, so Batch steps can't run in an
EXPRESS workflow or an EXPRESS Distributed Map child.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from sfnkit.compute.base import (
    PAYLOAD_JSON,
    ComputeBackend,
    ComputeSpec,
    SynthContext,
    TaskState,
    register_backend,
)
from sfnkit.runtime import protocol as P

if TYPE_CHECKING:
    from sfnkit.app import FunctionSpec

FARGATE_VCPUS = (0.25, 0.5, 1, 2, 4, 8, 16)
MAX_ARRAY_SIZE = 10_000

# Step Functions reports Batch API errors as "Batch.<exception>". These are safe to resubmit.
TRANSIENT_ERRORS = ["Batch.ServerException", "Batch.TooManyRequestsException"]


@dataclass(frozen=True)
class FargateQueue:
    """A Fargate (or Fargate Spot) compute environment and job queue that sfnkit creates in the stack.

    Steps whose ``Batch(queue=...)`` are equal share one queue. ``assign_public_ip`` is for public
    subnets without a NAT gateway: the tasks need some route to ECR, S3 and Step Functions.
    """

    subnets: tuple[str, ...]
    security_groups: tuple[str, ...]
    spot: bool = True
    max_vcpus: int = 256
    assign_public_ip: bool = True

    platform = "FARGATE"

    def __post_init__(self):
        object.__setattr__(self, "subnets", tuple(self.subnets))
        object.__setattr__(self, "security_groups", tuple(self.security_groups))
        if not self.subnets or not self.security_groups:
            raise ValueError("FargateQueue needs at least one subnet and one security group")
        if self.max_vcpus < 1:
            raise ValueError("FargateQueue max_vcpus must be at least 1")

    @property
    def key(self) -> str:
        raw = json.dumps([self.subnets, self.security_groups, self.spot, self.max_vcpus])
        return hashlib.sha256(raw.encode()).hexdigest()[:8]

    @property
    def env_id(self) -> str:
        return f"BatchEnv{self.key}"

    @property
    def queue_id(self) -> str:
        return f"BatchQueue{self.key}"


@dataclass(frozen=True)
class BatchQueue:
    """An existing job queue, made elsewhere (CDK, Terraform, the console). ``platform`` must match its
    compute environments: job definitions differ between Fargate and EC2."""

    arn: str
    platform: Literal["FARGATE", "EC2"] = "FARGATE"
    assign_public_ip: bool = False

    def __post_init__(self):
        if self.platform not in ("FARGATE", "EC2"):
            raise ValueError("BatchQueue platform must be FARGATE or EC2")


@dataclass(eq=False)
class Batch(ComputeSpec):
    """Run a step as an AWS Batch job.

    ``memory`` is MiB. ``timeout`` bounds each attempt's run time (queue time is not counted).
    ``retries`` is the number of attempts Batch makes for infrastructure failures. ``items_per_job``
    makes each array child process that many items, for lists over 10,000 or very small items.
    """

    kind = "batch"

    queue: FargateQueue | BatchQueue
    vcpus: float = 1
    memory: int = 2048
    gpus: int = 0
    timeout: int = 3600
    retries: int = 3
    ephemeral_storage: int | None = None
    items_per_job: int = 1
    env: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.queue, (FargateQueue, BatchQueue)):
            raise TypeError("Batch(queue=...) takes a FargateQueue or a BatchQueue")
        if self.fargate:
            if self.vcpus not in FARGATE_VCPUS:
                raise ValueError(f"Fargate jobs take vcpus in {FARGATE_VCPUS}")
            if self.gpus:
                raise ValueError("Fargate jobs can't use GPUs; use an EC2 BatchQueue")
            if self.ephemeral_storage is not None and not 21 <= self.ephemeral_storage <= 200:
                raise ValueError("Fargate ephemeral_storage must be 21..200 GiB")
        elif self.ephemeral_storage is not None:
            raise ValueError("ephemeral_storage is for Fargate; size EC2 disks with a launch template")
        if self.timeout < 60:
            raise ValueError("Batch timeout must be at least 60 seconds")
        if not 1 <= self.retries <= 10:
            raise ValueError("Batch retries must be 1..10")
        if self.items_per_job < 1:
            raise ValueError("items_per_job must be at least 1")

    @property
    def fargate(self) -> bool:
        return self.queue.platform == "FARGATE"

    def __repr__(self):  # part of the function identity, so keep it stable and complete
        return (
            f"Batch(queue={self.queue!r}, vcpus={self.vcpus}, memory={self.memory}, gpus={self.gpus}, "
            f"timeout={self.timeout}, retries={self.retries}, ephemeral_storage={self.ephemeral_storage}, "
            f"items_per_job={self.items_per_job}, env={sorted(self.env.items())})"
        )


def _tag_map(ctx: SynthContext) -> dict[str, str]:
    # Batch resources take tags as a map, unlike IAM and Logs.
    return {t["Key"]: t["Value"] for t in ctx.tags()}


def _queue_arn(spec: Batch) -> Any:
    q = spec.queue
    return {"Ref": q.queue_id} if isinstance(q, FargateQueue) else q.arn


class BatchBackend(ComputeBackend):
    spec_type = Batch
    assume_role_service = "ecs-tasks.amazonaws.com"  # the job role, which the container runs as
    image_runtime = "container"
    standard_only = True
    supports_array = True

    def resources(self, fn: FunctionSpec, ctx: SynthContext) -> dict[str, dict]:
        spec: Batch = fn.compute  # type: ignore[assignment]
        lid = fn.logical_id
        logs, exec_role = f"{lid}Logs", f"{lid}Exec"
        reqs = [
            {"Type": "VCPU", "Value": f"{spec.vcpus:g}"},
            {"Type": "MEMORY", "Value": str(spec.memory)},
        ]
        if spec.gpus:
            reqs.append({"Type": "GPU", "Value": str(spec.gpus)})
        env = {**ctx.runtime_env(fn), **spec.env, **fn.env}
        container: dict[str, Any] = {
            "Image": ctx.image_uri(fn.image),
            "JobRoleArn": ctx.role_arn(fn),
            "ExecutionRoleArn": {"Fn::GetAtt": [exec_role, "Arn"]},
            "ResourceRequirements": reqs,
            "Environment": [{"Name": k, "Value": v} for k, v in env.items()],
            "LogConfiguration": {
                "LogDriver": "awslogs",
                "Options": {
                    "awslogs-group": {"Ref": logs},
                    "awslogs-region": {"Ref": "AWS::Region"},
                    "awslogs-stream-prefix": "sfnkit",
                },
            },
        }
        if spec.fargate:
            container["NetworkConfiguration"] = {
                "AssignPublicIp": "ENABLED" if spec.queue.assign_public_ip else "DISABLED"
            }
            container["FargatePlatformConfiguration"] = {"PlatformVersion": "LATEST"}
            container["RuntimePlatform"] = {
                "CpuArchitecture": "ARM64" if fn.image.architecture == "arm64" else "X86_64",
                "OperatingSystemFamily": "LINUX",
            }
            if spec.ephemeral_storage is not None:
                container["EphemeralStorage"] = {"SizeInGiB": spec.ephemeral_storage}
        out: dict[str, dict] = {
            # Declared explicitly so `destroy` removes it; the default /aws/batch/job group never goes away.
            logs: {
                "Type": "AWS::Logs::LogGroup",
                "Properties": {"RetentionInDays": ctx.log_retention_days, "Tags": ctx.tags()},
            },
            exec_role: {
                # Pulls the image and writes logs on the job's behalf; the step itself runs as the job role.
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                    "ManagedPolicyArns": [
                        {
                            "Fn::Sub": "arn:${AWS::Partition}:iam::aws:policy/service-role/"
                            "AmazonECSTaskExecutionRolePolicy"
                        }
                    ],
                    "Tags": ctx.tags(),
                },
            },
            lid: {
                "Type": "AWS::Batch::JobDefinition",
                "Properties": {
                    "JobDefinitionName": {"Fn::Sub": f"${{AWS::StackName}}-{lid}"},
                    "Type": "container",
                    "PlatformCapabilities": [spec.queue.platform],
                    "Timeout": {"AttemptDurationSeconds": spec.timeout},
                    "RetryStrategy": {
                        "Attempts": spec.retries,
                        # The first matching rule wins, and Batch retries a job no rule matches. So only
                        # failures that would repeat end the job; image pull errors, Spot reclaims and
                        # lost hosts are retried.
                        "EvaluateOnExit": [
                            # The step raised; the runtime already reported it to Step Functions by name.
                            {"OnExitCode": "1", "Action": "EXIT"},
                            {"OnReason": "OutOfMemoryError*", "Action": "EXIT"},
                            {"OnStatusReason": "Job attempt duration exceeded timeout*", "Action": "EXIT"},
                        ],
                    },
                    "ContainerProperties": container,
                    "Tags": _tag_map(ctx),
                },
            },
        }
        q = spec.queue
        if isinstance(q, FargateQueue):
            # Shared by every step on this queue: same logical ids, so the template keeps one copy.
            out[q.env_id] = {
                "Type": "AWS::Batch::ComputeEnvironment",
                "Properties": {
                    "Type": "MANAGED",
                    "State": "ENABLED",
                    "ComputeResources": {
                        "Type": "FARGATE_SPOT" if q.spot else "FARGATE",
                        "MaxvCpus": q.max_vcpus,
                        "Subnets": list(q.subnets),
                        "SecurityGroupIds": list(q.security_groups),
                    },
                    "Tags": _tag_map(ctx),
                },
            }
            out[q.queue_id] = {
                "Type": "AWS::Batch::JobQueue",
                "Properties": {
                    "Priority": 1,
                    "State": "ENABLED",
                    "ComputeEnvironmentOrder": [{"Order": 1, "ComputeEnvironment": {"Ref": q.env_id}}],
                    "Tags": _tag_map(ctx),
                },
            }
        return out

    def baseline_statements(self, fn: FunctionSpec, ctx: SynthContext) -> list[dict]:
        return [
            {
                # Task tokens carry no resource ARN, so these can't be scoped further.
                "Effect": "Allow",
                "Action": ["states:SendTaskSuccess", "states:SendTaskFailure", "states:SendTaskHeartbeat"],
                "Resource": "*",
            },
            {
                # A failing array child stops the rest of its array.
                "Effect": "Allow",
                "Action": "batch:TerminateJob",
                "Resource": {"Fn::Sub": "arn:${AWS::Partition}:batch:${AWS::Region}:${AWS::AccountId}:job/*"},
            },
        ]

    def _arguments(self, fn: FunctionSpec) -> tuple[dict, dict]:
        spec: Batch = fn.compute  # type: ignore[assignment]
        lid = fn.logical_id
        jd, subs = f"{lid}JobDef", {f"{lid}JobDef": {"Ref": lid}}
        queue: Any = _queue_arn(spec)
        if isinstance(queue, dict):
            subs[f"{lid}Queue"] = queue
            queue = f"${{{lid}Queue}}"
        args = {
            # Execution name first, so a job is easy to find from the run it belongs to.
            "JobName": "{% $substring($replace($states.context.Execution.Name & '-"
            + lid
            + "', /[^A-Za-z0-9_-]/, '_'), 0, 128) %}",
            "JobQueue": queue,
            "JobDefinition": f"${{{jd}}}",
            "ContainerOverrides": {
                "Environment": [
                    {"Name": P.ENV_ENVELOPE, "Value": PAYLOAD_JSON},
                    {"Name": P.ENV_TOKEN, "Value": "{% $states.context.Task.Token %}"},
                ]
            },
        }
        return args, subs

    def task_state(self, fn: FunctionSpec) -> TaskState:
        args, subs = self._arguments(fn)
        # No TimeoutSeconds: time spent waiting in the queue is unbounded by design; the job definition
        # bounds each attempt's run time.
        return TaskState(
            resource="arn:aws:states:::batch:submitJob.sync",
            arguments=args,
            result="$states.result",  # what the container sent with SendTaskSuccess
            substitutions=subs,
        )

    def array_task_state(self, fn: FunctionSpec, size: str) -> TaskState:
        args, subs = self._arguments(fn)
        args["ArrayProperties"] = {"Size": "{% " + size + " %}"}
        return TaskState(
            resource="arn:aws:states:::batch:submitJob.sync",
            arguments=args,
            result="$states.result",  # unused: the compiler builds the array result from S3 locations
            substitutions=subs,
        )

    def state_machine_statements(self, fn: FunctionSpec) -> list[dict]:
        spec: Batch = fn.compute  # type: ignore[assignment]
        name = {
            "Fn::Sub": f"arn:${{AWS::Partition}}:batch:${{AWS::Region}}:${{AWS::AccountId}}:"
            f"job-definition/${{AWS::StackName}}-{fn.logical_id}:*"
        }
        return [
            {"Effect": "Allow", "Action": "batch:SubmitJob", "Resource": [name, _queue_arn(spec)]},
            # Job ids exist only at run time, so describe/terminate can't be scoped to a resource.
            {"Effect": "Allow", "Action": ["batch:DescribeJobs", "batch:TerminateJob"], "Resource": "*"},
            {
                # .sync Batch tasks are tracked through this managed EventBridge rule.
                "Effect": "Allow",
                "Action": ["events:PutTargets", "events:PutRule", "events:DescribeRule"],
                "Resource": {
                    "Fn::Sub": "arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:rule/"
                    "StepFunctionsGetEventsForBatchJobsRule"
                },
            },
        ]

    def default_retry(self) -> list[dict]:
        return [
            {
                "ErrorEquals": TRANSIENT_ERRORS,
                "IntervalSeconds": 2,
                "MaxAttempts": 3,
                "BackoffRate": 2.0,
                "JitterStrategy": "FULL",
            }
        ]


register_backend(BatchBackend)
