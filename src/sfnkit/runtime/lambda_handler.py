"""Lambda transport: the event is the envelope, the return value is the result wrapper.

An exception raised by the step reaches Step Functions with its class name as the error
(Lambda's ``errorType``), which is what ``Retry(on=...)`` and ``Err(...)`` patterns match on.
"""

from sfnkit.runtime.execute import execute


def handler(event, context):
    return execute(event)
