"""Task-token transport for container backends (ECS, Batch, EKS); no backend uses it yet.

The state machine starts the container with ``.waitForTaskToken``, passing the envelope in
``SFNKIT_ENVELOPE`` (JSON, or a ``{"$sw":1,"s3":...}`` wrapper when it's too big for an env var) and
the token in ``SFNKIT_TASK_TOKEN``. The result goes back with SendTaskSuccess/SendTaskFailure.
"""

from __future__ import annotations

import json
import os
import sys
import traceback

from sfnkit.runtime.execute import execute
from sfnkit.runtime.results import resolve

MAX_ERROR = 256
MAX_CAUSE = 32768


def main(*, environ=None, sfn=None, s3=None) -> int:
    environ = os.environ if environ is None else environ
    if sfn is None:
        import boto3

        sfn = boto3.client("stepfunctions")
    token = environ["SFNKIT_TASK_TOKEN"]
    try:
        envelope = json.loads(environ["SFNKIT_ENVELOPE"])
        if envelope.get("sw") is None:  # an S3 wrapper standing in for a large envelope
            envelope = resolve(envelope, s3)
        out = execute(envelope, s3=s3)
    except Exception as e:  # noqa: BLE001 - any failure must reach Step Functions
        # Same cause shape Lambda produces, so Err(...) handlers see one format everywhere.
        cause = {
            "errorMessage": str(e),
            "errorType": type(e).__name__,
            "stackTrace": traceback.format_exception(e),
        }
        sfn.send_task_failure(
            taskToken=token, error=type(e).__name__[:MAX_ERROR], cause=json.dumps(cause)[:MAX_CAUSE]
        )
        return 1
    sfn.send_task_success(taskToken=token, output=json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
