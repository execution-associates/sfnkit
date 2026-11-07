"""Start executions of a deployed workflow and read their results back as plain values."""

from __future__ import annotations

import json
import time
from typing import Any

from sfnkit.errors import SfnkitError
from sfnkit.runtime.results import _decode_cause, resolve

_RUNNING = {"RUNNING", "PENDING_REDRIVE"}


class ExecutionFailed(SfnkitError):
    def __init__(self, status: str, error: str | None, cause: Any, execution_arn: str | None = None):
        self.status, self.error, self.cause, self.execution_arn = status, error, cause, execution_arn
        super().__init__(f"execution {status}: {error or ''} {cause if cause is not None else ''}".strip())


def _session(session=None, region: str | None = None):
    if session is not None:
        return session
    import boto3

    return boto3.Session(region_name=region)


def start(
    state_machine_arn: str, input: Any = None, *, name: str | None = None, session=None, region=None
) -> str:
    sfn = _session(session, region).client("stepfunctions")
    args = {"stateMachineArn": state_machine_arn, "input": json.dumps(input if input is not None else {})}
    if name:
        args["name"] = name
    return sfn.start_execution(**args)["executionArn"]


def wait(
    execution_arn: str, *, poll: float = 2.0, timeout: float | None = None, session=None, region=None
) -> dict:
    sfn = _session(session, region).client("stepfunctions")
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        desc = sfn.describe_execution(executionArn=execution_arn)
        if desc["status"] not in _RUNNING:
            return desc
        if deadline is not None and time.monotonic() > deadline:
            raise TimeoutError(f"execution {execution_arn} still {desc['status']} after {timeout}s")
        time.sleep(poll)


def result(desc: dict, *, session=None, region=None) -> Any:
    """Resolved output of a finished execution, or ExecutionFailed with its error and cause."""
    if desc["status"] != "SUCCEEDED":
        raise ExecutionFailed(
            desc["status"], desc.get("error"), _decode_cause(desc.get("cause")), desc.get("executionArn")
        )
    out = json.loads(desc.get("output") or "null")
    return resolve(out, s3=_session(session, region).client("s3"))


def run(
    state_machine_arn: str,
    input: Any = None,
    *,
    express: bool = False,
    poll: float = 2.0,
    timeout: float | None = None,
    session=None,
    region=None,
) -> Any:
    session = _session(session, region)
    if express:
        # Express machines have no DescribeExecution; run synchronously instead.
        desc = session.client("stepfunctions").start_sync_execution(
            stateMachineArn=state_machine_arn, input=json.dumps(input if input is not None else {})
        )
        return result(desc, session=session)
    arn = start(state_machine_arn, input, session=session)
    return result(wait(arn, poll=poll, timeout=timeout, session=session), session=session)
