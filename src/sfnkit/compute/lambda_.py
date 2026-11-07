"""AWS Lambda backend: each (step, policy) becomes one container-image Lambda function."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sfnkit.compute.base import (
    PAYLOAD,
    ComputeBackend,
    ComputeSpec,
    SynthContext,
    TaskState,
    register_backend,
)

if TYPE_CHECKING:
    from sfnkit.app import FunctionSpec

HANDLER = "sfnkit.runtime.lambda_handler.handler"

# AWS's recommended retry for transient Lambda service errors.
TRANSIENT_ERRORS = [
    "Lambda.ClientExecutionTimeoutException",
    "Lambda.ServiceException",
    "Lambda.AWSLambdaException",
    "Lambda.SdkClientException",
    "Lambda.TooManyRequestsException",
]


@dataclass(eq=False)
class Lambda(ComputeSpec):
    kind = "lambda"

    memory: int = 512
    timeout: int = 60
    ephemeral_storage: int = 512
    env: dict[str, str] = field(default_factory=dict)
    reserved_concurrency: int | None = None

    def __post_init__(self):
        if not 128 <= self.memory <= 10240:
            raise ValueError("Lambda memory must be 128..10240 MB")
        if not 1 <= self.timeout <= 900:
            raise ValueError("Lambda timeout must be 1..900 seconds")
        if not 512 <= self.ephemeral_storage <= 10240:
            raise ValueError("Lambda ephemeral_storage must be 512..10240 MB")


class LambdaBackend(ComputeBackend):
    spec_type = Lambda
    assume_role_service = "lambda.amazonaws.com"

    def resources(self, fn: FunctionSpec, ctx: SynthContext) -> dict[str, dict]:
        spec: Lambda = fn.compute  # type: ignore[assignment]
        log_id = f"{fn.logical_id}Logs"
        props: dict[str, Any] = {
            "PackageType": "Image",
            "Code": {"ImageUri": ctx.image_uri(fn.image)},
            "Role": ctx.role_arn(fn),
            "MemorySize": spec.memory,
            "Timeout": spec.timeout,
            "EphemeralStorage": {"Size": spec.ephemeral_storage},
            "Architectures": [fn.image.architecture],
            "ImageConfig": {"Command": [HANDLER]},
            "Environment": {"Variables": {**ctx.runtime_env(fn), **spec.env, **fn.env}},
            "LoggingConfig": {"LogGroup": {"Ref": log_id}},
            "Tags": ctx.tags(),
        }
        if spec.reserved_concurrency is not None:
            props["ReservedConcurrentExecutions"] = spec.reserved_concurrency
        return {
            # Declared explicitly so `destroy` removes it; Lambda would otherwise create an orphan.
            log_id: {
                "Type": "AWS::Logs::LogGroup",
                "Properties": {"RetentionInDays": ctx.log_retention_days, "Tags": ctx.tags()},
            },
            fn.logical_id: {"Type": "AWS::Lambda::Function", "Properties": props},
        }

    def baseline_statements(self, fn: FunctionSpec, ctx: SynthContext) -> list[dict]:
        return [
            {
                "Effect": "Allow",
                "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": {"Fn::Sub": f"${{{fn.logical_id}Logs.Arn}}:*"},
            }
        ]

    def task_state(self, fn: FunctionSpec) -> TaskState:
        spec: Lambda = fn.compute  # type: ignore[assignment]
        placeholder = f"{fn.logical_id}Arn"
        return TaskState(
            resource="arn:aws:states:::lambda:invoke",
            arguments={"FunctionName": f"${{{placeholder}}}", "Payload": PAYLOAD},
            result="$states.result.Payload",
            # A little slack over the function timeout so Lambda's own timeout error surfaces first.
            timeout_seconds=spec.timeout + 30,
            substitutions={placeholder: {"Fn::GetAtt": [fn.logical_id, "Arn"]}},
        )

    def state_machine_statements(self, fn: FunctionSpec) -> list[dict]:
        arn = {"Fn::GetAtt": [fn.logical_id, "Arn"]}
        return [
            {
                "Effect": "Allow",
                "Action": "lambda:InvokeFunction",
                "Resource": [arn, {"Fn::Sub": f"${{{fn.logical_id}.Arn}}:*"}],
            }
        ]

    def default_retry(self) -> list[dict]:
        return [
            {
                "ErrorEquals": TRANSIENT_ERRORS,
                "IntervalSeconds": 1,
                "MaxAttempts": 3,
                "BackoffRate": 2.0,
                "JitterStrategy": "FULL",
            }
        ]


register_backend(LambdaBackend)
