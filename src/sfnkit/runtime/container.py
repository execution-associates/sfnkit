"""Task-token transport for container backends (Batch now; ECS and EKS work the same way).

The state machine starts the container with the envelope in ``SFNKIT_ENVELOPE`` and its task token in
``SFNKIT_TASK_TOKEN``. Batch passes the token on a ``.sync`` submit; Step Functions takes the first
SendTaskSuccess/SendTaskFailure as the task's outcome and stops watching the job.

One job: run the step, then SendTaskSuccess with its result wrapper, or SendTaskFailure with the
exception's class name, the same error shape Lambda produces.

Array job child (the envelope has ``"array"``): run items ``[i*k, (i+1)*k)`` and write each result
wrapper to S3. Success needs no callback, since the task completes when the whole array job does.

  * Items that already have a result in this run are skipped, so a retry or redrive only runs what
    didn't finish.
  * Without a failure budget, an exception fails the task by name and terminates the array.
  * With one (``tolerate``), a failed item is recorded and the child carries on. The task fails with
    ``ToleratedFailureExceeded`` only once the recorded failures of this submission pass the budget.
  * Children past the end of the list (Batch arrays have at least two) exit at once.

Exit status: 0 when the outcome was reported, 1 when the step failed. Batch's retry rules only retry
lost infrastructure, so a step failure is never re-run by Batch.
"""

from __future__ import annotations

import json
import os
import sys
import traceback

from sfnkit.errors import ToleratedFailureExceeded
from sfnkit.runtime import protocol as P
from sfnkit.runtime.execute import execute
from sfnkit.runtime.results import _put_json, _s3, array_items, exists, resolve

MAX_ERROR = 256
MAX_CAUSE = 32768

# The task already finished: another array child failed it first, or it timed out.
_CLOSED = {"TaskTimedOut", "TaskDoesNotExist", "InvalidToken"}


def _send(call, **kwargs) -> None:
    try:
        call(**kwargs)
    except Exception as e:  # noqa: BLE001 - only a closed task is expected; anything else must surface
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if code not in _CLOSED:
            raise
        print(f"sfnkit: task already closed ({code}); outcome not reported", file=sys.stderr)


def _cause(e: BaseException) -> str:
    # Same cause shape Lambda produces, so Err(...) handlers see one format everywhere.
    cause = {
        "errorMessage": str(e),
        "errorType": type(e).__name__,
        "stackTrace": traceback.format_exception(e),
    }
    return json.dumps(cause)[:MAX_CAUSE]


def _stop_array(environ, batch) -> None:
    """Terminate the rest of this child's array job (best effort)."""
    job = environ.get("AWS_BATCH_JOB_ID", "")
    parent = job.split(":")[0]
    if not parent or ":" not in job:
        return
    try:
        if batch is None:
            import boto3

            batch = boto3.client("batch")
        batch.terminate_job(jobId=parent, reason="sfnkit: an array child failed")
    except Exception as e:  # noqa: BLE001 - the failure is already reported; this only saves compute
        print(f"sfnkit: couldn't terminate array job {parent}: {e}", file=sys.stderr)


def _count(bucket: str, prefix: str, enough: int, s3) -> int:
    """Objects under ``prefix``, counting no further than ``enough``."""
    n = 0
    for page in _s3(s3).get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        n += page.get("KeyCount", 0)
        if n >= enough:
            break
    return n


def _run_array(envelope: dict, environ, s3) -> None:
    arr = envelope["array"]
    index = int(environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0"))
    per, n = int(arr.get("per", 1)), int(arr["items"]["n"])
    start, stop = index * per, min((index + 1) * per, n)
    if start >= stop:
        return
    bucket = environ.get(P.ENV_BUCKET)
    if not bucket:
        raise RuntimeError(f"array job results need {P.ENV_BUCKET}")
    execution, node = envelope["execution"], envelope["node"]
    tolerate = arr.get("tolerate")
    job = environ.get("AWS_BATCH_JOB_ID", "local").split(":")[0]
    base = {k: v for k, v in envelope.items() if k != "array"}
    failed: list[tuple[int, BaseException]] = []
    for j, item in zip(range(start, stop), array_items(arr["items"], start, stop, s3)):
        key = P.results_key(execution, node, j)
        if exists(bucket, key, s3):  # finished by an earlier attempt of this run
            continue
        one = {**base, "args": {**(base.get("args") or {}), arr["param"]: item}, "index": j}
        try:
            out = execute(one, s3=s3)
        except Exception as e:
            if tolerate is None:
                raise
            record = {"index": j, "error": type(e).__name__, "cause": json.loads(_cause(e))}
            _put_json(bucket, f"{P.errors_prefix(execution, node, job)}{j}.json", record, s3)
            failed.append((j, e))
            continue
        _put_json(bucket, key, out, s3)
    if failed:
        # Written before counting, so of two children failing at once at least one sees both.
        count = _count(bucket, P.errors_prefix(execution, node, job), int(tolerate) + 1, s3)
        if count > int(tolerate):
            j, e = failed[0]
            raise ToleratedFailureExceeded(
                f"more than {tolerate} of {n} items failed; item {j}: {type(e).__name__}: {e}"
            ) from e


def main(*, environ=None, sfn=None, s3=None, batch=None) -> int:
    environ = os.environ if environ is None else environ
    if sfn is None:
        import boto3

        sfn = boto3.client("stepfunctions")
    token = environ[P.ENV_TOKEN]
    envelope: dict = {}
    try:
        envelope = json.loads(environ[P.ENV_ENVELOPE])
        if envelope.get("sw") is None:  # an S3 wrapper standing in for a large envelope
            envelope = resolve(envelope, s3)
        if envelope.get("array"):
            _run_array(envelope, environ, s3)
            return 0
        out = execute(envelope, s3=s3)
    except Exception as e:  # noqa: BLE001 - any failure must reach Step Functions
        _send(sfn.send_task_failure, taskToken=token, error=type(e).__name__[:MAX_ERROR], cause=_cause(e))
        if envelope.get("array"):
            _stop_array(environ, batch)
        return 1
    _send(sfn.send_task_success, taskToken=token, output=json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
