"""CloudFormation templates for an ``App``.

Two stacks per workflow:
  * ``<stack>-assets``: the ECR repository and the artifacts bucket. They must exist before images are
    pushed and before functions can reference them.
  * ``<stack>``: per-function roles + compute resources (from the backend), per-state-machine roles and
    state machines.
"""

from __future__ import annotations

from typing import Any

from sfnkit.app import App, FunctionSpec, ImageVariant, SmNeed, StateMachineSpec, sm_arn, sm_full_name
from sfnkit.compute.base import backend_for
from sfnkit.runtime import protocol as P

VERSION = "2010-09-09"
POLICY_VERSION = "2012-10-17"
BUCKET_PARAM = "ArtifactsBucket"


def tags(app: App) -> list[dict[str, str]]:
    return [
        {"Key": "sfnkit:workflow", "Value": app.name},
        {"Key": "sfnkit:managed", "Value": "true"},
    ]


def _bucket_arn(suffix: str = "") -> dict:
    return {"Fn::Sub": f"arn:${{AWS::Partition}}:s3:::${{{BUCKET_PARAM}}}{suffix}"}


def _states_arn(kind: str, rest: str) -> dict:
    return {
        "Fn::Sub": f"arn:${{AWS::Partition}}:states:${{AWS::Region}}:${{AWS::AccountId}}:{kind}:"
        f"${{AWS::StackName}}-{rest}"
    }


def _assume(service: str) -> dict:
    return {
        "Version": POLICY_VERSION,
        "Statement": [{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}],
    }


def _role(app: App, service: str, statements: list[dict]) -> dict:
    props: dict[str, Any] = {"AssumeRolePolicyDocument": _assume(service), "Tags": tags(app)}
    if statements:  # IAM rejects an inline policy with no statements
        props["Policies"] = [
            {"PolicyName": "sfnkit", "PolicyDocument": {"Version": POLICY_VERSION, "Statement": statements}}
        ]
    return {"Type": "AWS::IAM::Role", "Properties": props}


# ------------------------------------------------------------------------------------------------ assets


def assets_template(app: App) -> dict[str, Any]:
    days = app.settings.run_retention_days
    return {
        "AWSTemplateFormatVersion": VERSION,
        "Description": f"sfnkit assets for workflow {app.name} (images and run artifacts)",
        "Resources": {
            "Repository": {
                "Type": "AWS::ECR::Repository",
                "Properties": {
                    # Lets `destroy` delete the repository without deleting every image first.
                    "EmptyOnDelete": True,
                    "ImageScanningConfiguration": {"ScanOnPush": False},
                    "Tags": tags(app),
                },
            },
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketEncryption": {
                        "ServerSideEncryptionConfiguration": [
                            {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                        ]
                    },
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "BlockPublicPolicy": True,
                        "IgnorePublicAcls": True,
                        "RestrictPublicBuckets": True,
                    },
                    "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
                    "LifecycleConfiguration": {
                        "Rules": [
                            {
                                "Id": "expire-runs",
                                "Status": "Enabled",
                                "Prefix": P.RUNS_PREFIX,
                                "ExpirationInDays": days,
                            },
                            {
                                "Id": "expire-templates",
                                "Status": "Enabled",
                                "Prefix": "templates/",
                                "ExpirationInDays": 7,
                            },
                            {
                                "Id": "abort-uploads",
                                "Status": "Enabled",
                                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
                            },
                        ]
                    },
                    "Tags": tags(app),
                },
            },
        },
        "Outputs": {
            "RepositoryUri": {"Value": {"Fn::GetAtt": ["Repository", "RepositoryUri"]}},
            "RepositoryName": {"Value": {"Ref": "Repository"}},
            "BucketName": {"Value": {"Ref": "Bucket"}},
        },
    }


# -------------------------------------------------------------------------------------------------- main


class _Ctx:
    """``SynthContext`` for backends writing their resources into the main template."""

    def __init__(self, app: App):
        self.app = app
        self.log_retention_days = app.settings.log_retention_days

    def image_uri(self, image: ImageVariant) -> Any:
        return {"Ref": image.param_name}

    def role_arn(self, fn: FunctionSpec) -> Any:
        return {"Fn::GetAtt": [role_id(fn), "Arn"]}

    def runtime_env(self, fn: FunctionSpec) -> dict[str, Any]:
        return {
            P.ENV_MODULE: self.app.module,
            P.ENV_BUCKET: {"Ref": BUCKET_PARAM},
            P.ENV_THRESHOLD: str(self.app.settings.offload_threshold),
        }

    def tags(self) -> list[dict[str, str]]:
        return tags(self.app)


def role_id(fn: FunctionSpec) -> str:
    return f"{fn.logical_id}Role"


def _function_statements(fn: FunctionSpec, ctx: _Ctx) -> list[dict]:
    backend = backend_for(fn.compute)
    stmts = list(backend.baseline_statements(fn, ctx))
    stmts += [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": _bucket_arn(f"/{P.RUNS_PREFIX}*"),
        },
        # ListBucket makes a missing key a 404 instead of a 403.
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": _bucket_arn()},
    ]
    stmts += fn.policy.to_statements()
    return stmts


def _need_statements(app: App, sm: StateMachineSpec, need: SmNeed) -> list[dict]:
    if need.kind == "invoke":
        assert need.fn is not None
        return backend_for(need.fn.compute).state_machine_statements(need.fn)
    if need.kind == "start_child":
        assert need.child is not None
        return [
            {"Effect": "Allow", "Action": "states:StartExecution", "Resource": sm_arn(need.child)},
            {
                "Effect": "Allow",
                "Action": ["states:DescribeExecution", "states:StopExecution"],
                "Resource": _states_arn("execution", f"{need.child}:*"),
            },
            {
                # .sync child executions are tracked through this managed EventBridge rule.
                "Effect": "Allow",
                "Action": ["events:PutTargets", "events:PutRule", "events:DescribeRule"],
                "Resource": {
                    "Fn::Sub": "arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:rule/"
                    "StepFunctionsGetEventsForStepFunctionsExecutionRule"
                },
            },
        ]
    if need.kind == "map_run":
        return [
            {"Effect": "Allow", "Action": "states:StartExecution", "Resource": sm_arn(sm.name)},
            {
                "Effect": "Allow",
                "Action": ["states:DescribeExecution", "states:StopExecution"],
                # Distributed Map children are named <state machine>/<map run label>:<id>.
                "Resource": [
                    _states_arn("execution", f"{sm.name}:*"),
                    _states_arn("execution", f"{sm.name}/*"),
                ],
            },
            {
                "Effect": "Allow",
                "Action": ["states:DescribeMapRun", "states:UpdateMapRun", "states:ListMapRuns"],
                "Resource": [_states_arn("mapRun", f"{sm.name}/*"), _states_arn("execution", f"{sm.name}:*")],
            },
        ]
    if need.kind == "bucket":
        return [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                "Resource": _bucket_arn(f"/{P.RUNS_PREFIX}*"),
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads"],
                "Resource": _bucket_arn(),
            },
        ]
    if need.kind == "s3_read":
        assert need.bucket
        prefix = need.prefix or ""
        return [
            {
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": {"Fn::Sub": f"arn:${{AWS::Partition}}:s3:::{need.bucket}/{prefix}*"},
            },
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": {"Fn::Sub": f"arn:${{AWS::Partition}}:s3:::{need.bucket}"},
            },
        ]
    raise ValueError(f"unknown state machine need {need.kind!r}")


def state_machine_role_statements(app: App, sm: StateMachineSpec) -> list[dict]:
    out: list[dict] = []
    for need in sm.needs:
        for st in _need_statements(app, sm, need):
            if st not in out:
                out.append(st)
    return out


def main_template(app: App) -> dict[str, Any]:
    ctx = _Ctx(app)
    params: dict[str, Any] = {
        BUCKET_PARAM: {"Type": "String", "Description": "Artifacts bucket from the assets stack"}
    }
    for image in app.images:
        params[image.param_name] = {"Type": "String", "Description": f"Image URI for variant {image.key}"}

    resources: dict[str, Any] = {}
    for fn in app.functions:
        backend = backend_for(fn.compute)
        params.update(backend.parameters(fn))
        resources[role_id(fn)] = _role(app, backend.assume_role_service, _function_statements(fn, ctx))
        resources.update(backend.resources(fn, ctx))

    by_name = {sm.name: sm for sm in app.state_machines}
    for sm in app.state_machines:
        rid = f"{sm.logical_id}Role"
        resources[rid] = _role(app, "states.amazonaws.com", state_machine_role_statements(app, sm))
        props: dict[str, Any] = {
            "StateMachineName": sm_full_name(sm.name),
            "StateMachineType": sm.type,
            "Definition": sm.definition,
            "RoleArn": {"Fn::GetAtt": [rid, "Arn"]},
            "Tags": tags(app),
        }
        if sm.substitutions:
            props["DefinitionSubstitutions"] = sm.substitutions
        res: dict[str, Any] = {"Type": "AWS::StepFunctions::StateMachine", "Properties": props}
        # Children are referenced by constructed ARN, so create them first explicitly.
        deps = sorted(
            {by_name[n.child].logical_id for n in sm.needs if n.kind == "start_child" and n.child in by_name}
        )
        if deps:
            res["DependsOn"] = deps
        resources[sm.logical_id] = res

    outputs = {
        "StateMachineArn": {"Value": {"Ref": app.root.logical_id}},
        "StateMachineType": {"Value": app.root.type},
    }
    return {
        "AWSTemplateFormatVersion": VERSION,
        "Description": f"sfnkit workflow {app.name}",
        "Parameters": params,
        "Resources": resources,
        "Outputs": outputs,
    }
