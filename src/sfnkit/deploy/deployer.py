"""Deploy and destroy an ``App``: assets stack, then images, then the main stack."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import ClientError, WaiterError

from sfnkit.app import App
from sfnkit.compile.cfn import BUCKET_PARAM, assets_template, main_template, tags
from sfnkit.errors import DeployError

MAX_INLINE_TEMPLATE = 51_200
MAX_SM_NAME = 80
_STACK_RE = re.compile(r"^[A-Za-z][A-Za-z0-9-]*$")
_NO_CHANGES = ("didn't contain changes", "No updates are to be performed")
_WAIT = {"Delay": 5, "MaxAttempts": 720}


@dataclass
class DeployResult:
    stack_name: str
    state_machine_arn: str
    bucket: str
    region: str


def default_stack_name(app: App) -> str:
    return f"sfnkit-{app.name}"


def assets_stack_name(stack_name: str) -> str:
    return f"{stack_name}-assets"


def validate_stack_name(stack_name: str, app: App | None = None) -> None:
    if not _STACK_RE.match(stack_name):
        raise DeployError(
            f"stack name {stack_name!r} must start with a letter and use only letters, digits and -"
        )
    if len(assets_stack_name(stack_name)) > 128:
        raise DeployError(f"stack name {stack_name!r} is too long (max {128 - len('-assets')} characters)")
    if app is not None:
        for sm in app.state_machines:
            full = f"{stack_name}-{sm.name}"
            if len(full) > MAX_SM_NAME:
                raise DeployError(
                    f"state machine name {full!r} is {len(full)} characters (Step Functions allows "
                    f"{MAX_SM_NAME}); use a shorter --stack-name"
                )


def _session(session=None, region: str | None = None):
    if session is not None:
        return session
    import boto3

    return boto3.Session(region_name=region)


def _stack_status(cfn, name: str) -> str | None:
    try:
        stacks = cfn.describe_stacks(StackName=name)["Stacks"]
    except ClientError as e:
        if "does not exist" in str(e):
            return None
        raise
    return stacks[0]["StackStatus"] if stacks else None


def stack_outputs(cfn, name: str) -> dict[str, str]:
    stacks = cfn.describe_stacks(StackName=name)["Stacks"]
    return {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}


def _failure_reasons(cfn, name: str, limit: int = 10) -> str:
    try:
        events = cfn.describe_stack_events(StackName=name)["StackEvents"]
    except ClientError:
        return "(no stack events available)"
    lines = [
        f"  {e['LogicalResourceId']}: {e.get('ResourceStatusReason', e['ResourceStatus'])}"
        for e in events
        if e["ResourceStatus"].endswith("_FAILED")
    ]
    return "\n".join(lines[:limit]) or "(no failed resources reported)"


def _wait(cfn, waiter: str, name: str) -> None:
    try:
        cfn.get_waiter(waiter).wait(StackName=name, WaiterConfig=_WAIT)
    except WaiterError:
        status = _stack_status(cfn, name)
        raise DeployError(f"stack {name} ended in {status}:\n{_failure_reasons(cfn, name)}") from None


def deploy_stack(
    cfn,
    name: str,
    *,
    template_body: str | None = None,
    template_url: str | None = None,
    parameters: dict[str, str] | None = None,
    capabilities: list[str] | None = None,
    stack_tags: list[dict] | None = None,
    log: Callable = print,
) -> dict[str, str]:
    """Create or update a stack through a change set; an empty change set counts as success."""
    status = _stack_status(cfn, name)
    if status == "ROLLBACK_COMPLETE":
        # A stack whose first create failed can only be deleted, not updated.
        log(f"{name}: removing failed previous create")
        cfn.delete_stack(StackName=name)
        _wait(cfn, "stack_delete_complete", name)
        status = None
    if status and status.endswith("_IN_PROGRESS"):
        raise DeployError(f"stack {name} is busy ({status}); try again when it settles")
    change_type = "UPDATE" if status else "CREATE"
    cs_name = f"sfnkit-{int(time.time() * 1000)}"
    args: dict[str, Any] = {
        "StackName": name,
        "ChangeSetName": cs_name,
        "ChangeSetType": change_type,
        "Parameters": [{"ParameterKey": k, "ParameterValue": v} for k, v in (parameters or {}).items()],
        "Capabilities": capabilities or [],
        "Tags": stack_tags or [],
    }
    if template_url:
        args["TemplateURL"] = template_url
    else:
        args["TemplateBody"] = template_body
    cfn.create_change_set(**args)
    try:
        cfn.get_waiter("change_set_create_complete").wait(
            StackName=name, ChangeSetName=cs_name, WaiterConfig={"Delay": 3, "MaxAttempts": 200}
        )
    except WaiterError:
        desc = cfn.describe_change_set(StackName=name, ChangeSetName=cs_name)
        reason = desc.get("StatusReason", "")
        if any(s in reason for s in _NO_CHANGES):
            cfn.delete_change_set(StackName=name, ChangeSetName=cs_name)
            log(f"{name}: no changes")
            return stack_outputs(cfn, name)
        raise DeployError(f"change set for {name} failed: {reason}") from None
    log(f"{name}: {'creating' if change_type == 'CREATE' else 'updating'}")
    cfn.execute_change_set(StackName=name, ChangeSetName=cs_name)
    _wait(cfn, "stack_create_complete" if change_type == "CREATE" else "stack_update_complete", name)
    return stack_outputs(cfn, name)


def deploy(
    app: App,
    *,
    stack_name: str | None = None,
    region: str | None = None,
    session=None,
    builder=None,
    queues: dict[str, str] | None = None,
    log: Callable = print,
) -> DeployResult:
    """``queues`` binds named ``BatchQueue("gpu")`` queues to job queue ARNs."""
    from sfnkit.build.context import prepare_context
    from sfnkit.build.docker import DockerBuilder

    queue_params = bind_queues(app, queues or {})
    session = _session(session, region)
    stack = stack_name or default_stack_name(app)
    validate_stack_name(stack, app)
    cfn = session.client("cloudformation")
    stack_tags = tags(app)

    assets = deploy_stack(
        cfn,
        assets_stack_name(stack),
        template_body=json.dumps(assets_template(app)),
        stack_tags=stack_tags,
        log=log,
    )
    bucket, repo_uri = assets["BucketName"], assets["RepositoryUri"]

    builder = builder or DockerBuilder(log=log)
    ecr = session.client("ecr")
    params = {BUCKET_PARAM: bucket, **queue_params}
    for image in app.images:
        context = prepare_context(app, image)
        params[image.param_name] = builder.ensure_image(context, repo_uri, ecr)

    body = json.dumps(main_template(app))
    template_url = None
    if len(body) > MAX_INLINE_TEMPLATE:
        key = f"templates/{stack}-{hashlib.sha256(body.encode()).hexdigest()[:16]}.json"
        session.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode())
        template_url = f"https://{bucket}.s3.{session.region_name}.amazonaws.com/{key}"

    outputs = deploy_stack(
        cfn,
        stack,
        template_body=None if template_url else body,
        template_url=template_url,
        parameters=params,
        capabilities=["CAPABILITY_IAM"],
        stack_tags=stack_tags,
        log=log,
    )
    return DeployResult(stack, outputs["StateMachineArn"], bucket, session.region_name)


def bind_queues(app: App, queues: dict[str, str]) -> dict[str, str]:
    """Template parameters for named Batch queues; every name the workflow uses must be bound."""
    from sfnkit.compute.batch import named_queues

    named = named_queues(app.functions)
    unknown = sorted(set(queues) - set(named))
    if unknown:
        raise DeployError(f"--queue {', '.join(unknown)}: no step runs on a queue by that name")
    missing = sorted(set(named) - set(queues))
    if missing:
        flags = " ".join(f"--queue {n}=<job queue ARN>" for n in missing)
        raise DeployError(
            f"steps run on the named Batch queue(s) {', '.join(missing)}; bind them with {flags} "
            "(or deploy through sfnkit.cdk.SfnkitWorkflow(queues=...))"
        )
    for name, arn in queues.items():
        if not arn.startswith("arn:"):
            raise DeployError(f"--queue {name}={arn}: expected a job queue ARN")
    return {named[n].param_id: arn for n, arn in queues.items()}


def empty_bucket(s3, bucket: str) -> None:
    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket):
        objs = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in page.get("Versions", [])]
        objs += [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in page.get("DeleteMarkers", [])]
        for i in range(0, len(objs), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs[i : i + 1000], "Quiet": True})


def _delete_stack(cfn, name: str, log: Callable) -> None:
    if _stack_status(cfn, name) is None:
        return
    log(f"{name}: deleting")
    cfn.delete_stack(StackName=name)
    _wait(cfn, "stack_delete_complete", name)


def destroy(
    target: App | str,
    *,
    stack_name: str | None = None,
    region: str | None = None,
    session=None,
    log: Callable = print,
) -> None:
    """Delete the main stack, empty the artifacts bucket, delete the assets stack. Safe to re-run."""
    session = _session(session, region)
    stack = stack_name or (default_stack_name(target) if isinstance(target, App) else target)
    cfn = session.client("cloudformation")
    _delete_stack(cfn, stack, log)
    assets = assets_stack_name(stack)
    if _stack_status(cfn, assets) is None:
        return
    bucket = stack_outputs(cfn, assets).get("BucketName")
    if bucket:
        try:
            empty_bucket(session.client("s3"), bucket)
        except ClientError as e:
            if "NoSuchBucket" not in str(e):
                raise
    _delete_stack(cfn, assets, log)
