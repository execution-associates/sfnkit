"""Transport-free step execution: envelope in, result wrapper out.

Every compute backend's transport (Lambda event/return, a container's task token) ends up here.
"""

from __future__ import annotations

import importlib
import os
from typing import Any

from sfnkit.runtime import protocol as P
from sfnkit.runtime.results import resolve, threshold_from_env, wrap

_imported: set[str] = set()


def _import_workflow_module() -> None:
    # The workflow module imports every step it uses, which fills the step registry.
    mod = os.environ.get(P.ENV_MODULE)
    if mod and mod not in _imported:
        importlib.import_module(mod)
        _imported.add(mod)


def execute(
    envelope: dict[str, Any],
    *,
    s3=None,
    bucket: str | None = None,
    threshold: int | None = None,
) -> dict[str, Any]:
    from sfnkit.step import lookup

    if envelope.get("sw") != P.VERSION:
        raise ValueError(f"unsupported sfnkit envelope version: {envelope.get('sw')!r}")
    _import_workflow_module()
    step = lookup(envelope["step"])
    args = {k: resolve(v, s3) for k, v in (envelope.get("args") or {}).items()}
    # Exceptions propagate unchanged: the transport reports their class name as the error.
    result = step.fn(**args)
    return wrap(
        result,
        execution=envelope.get("execution") or "local",
        node=envelope.get("node") or "node",
        bucket=bucket or os.environ.get(P.ENV_BUCKET),
        threshold=threshold or threshold_from_env(),
        facts=envelope.get("facts"),
        items=envelope.get("items"),
        s3=s3,
    )
