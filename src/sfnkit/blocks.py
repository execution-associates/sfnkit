"""Control-flow building blocks used inside a ``@workflow`` body.

* ``match attempt(fn, ...)`` with ``case Ok(value)`` / ``case Err(error, cause)``: error handling.
* ``with each(items) as it:`` ... ``it.result(x)`` ... ``it.value``: a Map with a multi-step body.
* ``with subworkflow("name"):``: run the block as a child state machine.
* ``wait(seconds=...)`` / ``wait(until=...)``, ``fail(error, cause)``.
* ``is_null``, ``one_of``, ``length``: conditions that Python syntax can't express on a traced value.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any

from sfnkit.errors import TraceError
from sfnkit.expr import Expr, Get, IsNull, IsOk, Length, Lit, Not, OneOf, OutcomeGet, _decide, lift
from sfnkit.iam import Policy
from sfnkit.trace.tracer import TNode, require_tracer


class States:
    """Step Functions' built-in error names, for ``Retry(on=[...])`` and ``Err(...)`` patterns."""

    ALL = "States.ALL"
    TIMEOUT = "States.Timeout"
    TASK_FAILED = "States.TaskFailed"
    PERMISSIONS = "States.Permissions"
    HEARTBEAT_TIMEOUT = "States.HeartbeatTimeout"


# attempt / Ok / Err


class _OutcomeMeta(type):
    def __instancecheck__(cls, inst):
        if isinstance(inst, OutcomeGet):
            cond = IsOk(Get(inst.src)) if cls is Ok else Not(IsOk(Get(inst.src)))
            return _decide(cond)
        if isinstance(inst, Expr):
            raise TraceError("Ok(...)/Err(...) patterns only match the outcome of attempt(...)")
        return type.__instancecheck__(cls, inst)


class Ok(metaclass=_OutcomeMeta):
    """``case Ok(value):`` the attempted work succeeded with ``value``."""

    __match_args__ = ("value",)

    def __init__(self, value: Any = None):
        self.value = value

    def __repr__(self):
        return f"Ok({self.value!r})"


class Err(metaclass=_OutcomeMeta):
    """``case Err(error, cause):`` the attempted work failed. ``error`` is the error name (a Python
    exception's class name), ``cause`` its details (a dict for Python exceptions)."""

    __match_args__ = ("error", "cause")

    def __init__(self, error: str = "", cause: Any = None):
        self.error, self.cause = error, cause

    def __repr__(self):
        return f"Err({self.error!r}, {self.cause!r})"


def attempt(fn, *args, **kwargs) -> OutcomeGet:
    """Run a step, a workflow or a plain function (a multi-step block) and catch its errors.

    Must be the subject of a ``match`` directly in the workflow body. Errors that no ``Err`` case
    matches are not caught and fail the workflow as they would without ``attempt``.
    """
    t = require_tracer("attempt()")
    return t.attempt(fn, args, kwargs, sys._getframe(1))


# each / map


class each:
    """``with each(items, distributed=False, max_concurrency=None) as it:`` run the block per item.

    Inside, ``it.item`` is the current item; call ``it.result(x)`` with the per-item value. After the
    block, ``it.value`` is the list of results in item order.
    """

    def __init__(
        self,
        items,
        *,
        distributed: bool = False,
        max_concurrency: int | None = None,
        tolerated_failure_percentage: float | None = None,
        tolerated_failure_count: int | None = None,
        child_type: str = "STANDARD",
    ):
        self._frame = sys._getframe(1)
        self._items = items
        self._opts = dict(
            distributed=distributed,
            max_concurrency=max_concurrency,
            tolerated_failure_percentage=tolerated_failure_percentage,
            tolerated_failure_count=tolerated_failure_count,
            child_type=child_type,
        )
        self._node: TNode | None = None
        self._result: Expr | None = None

    def __enter__(self) -> each:
        t = require_tracer("each()")
        self._node = t.each_enter(self._items, self._opts, self._frame)
        del self._frame
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            require_tracer("each()").each_exit(self._node, self._result)  # type: ignore[arg-type]
        return False

    @property
    def item(self) -> Get:
        if self._node is None or self._node.closed:
            raise TraceError("it.item is only available inside the `with each(...)` block")
        return Get(self._node.item)

    def result(self, value) -> None:
        if self._node is None or self._node.closed:
            raise TraceError("it.result() must be called inside the `with each(...)` block")
        self._result = lift(value)

    @property
    def value(self) -> Get:
        if self._node is None or not self._node.closed:
            raise TraceError("it.value is only available after the `with each(...)` block")
        return Get(self._node)


def _map_call(target, items, kwargs: dict, opts: dict, frame) -> Get:
    """``step.map(items, **kwargs)`` / ``workflow.map(...)``: a Map whose body is one call."""
    from sfnkit.workflow import Workflow

    t = require_tracer(f"{target.name}.map()")
    n = t.each_enter(items, opts, frame)
    param = target.item_param(kwargs)
    bound = target.bind((), {**kwargs, param: Get(n.item)})
    r = t.call(target, bound, frame) if isinstance(target, Workflow) else t.task(target, bound, frame)
    t.each_exit(n, r)
    return Get(n)


@dataclass(frozen=True)
class S3Objects:
    """Distributed Map items: the objects under ``s3://bucket/prefix`` (each item is a listing entry
    with ``Key``, ``Size``, ``ETag``, ``LastModified``)."""

    bucket: str
    prefix: str = ""


@dataclass(frozen=True)
class S3File:
    """Distributed Map items read from one S3 object: a JSON array, JSON Lines, or CSV with a header."""

    bucket: str
    key: str
    format: str = "json"

    def __post_init__(self):
        if self.format not in ("json", "jsonl", "csv"):
            raise ValueError("S3File format must be json, jsonl or csv")


# subworkflow


class subworkflow:
    """``with subworkflow("name", iam=None, type="STANDARD"):`` run the block as a child state machine.

    Values created inside can be used after the block. Steps inside without their own ``iam=``
    inherit this block's ``iam`` if given, otherwise the enclosing workflow's.
    """

    def __init__(self, name: str, *, iam: Policy | None = None, type: str = "STANDARD"):
        if type not in ("STANDARD", "EXPRESS"):
            raise ValueError("type must be STANDARD or EXPRESS")
        self._frame = sys._getframe(1)
        self._opts = {"name": name, "iam": iam, "type": type}
        self._node: TNode | None = None

    def __enter__(self):
        self._node = require_tracer("subworkflow()").sub_enter(self._opts, self._frame)
        del self._frame
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            require_tracer("subworkflow()").sub_exit(self._node)  # type: ignore[arg-type]
        return False


# wait / fail


def wait(*, seconds: int | Expr | None = None, until: str | Expr | None = None) -> None:
    """Pause the workflow. Everything traced before runs first; everything after waits."""
    require_tracer("wait()").wait(seconds, until, sys._getframe(1))


def fail(error: str | Expr = "Failed", cause: str | Expr = "") -> None:
    """End the workflow (or the enclosing attempt) with an error."""
    require_tracer("fail()").fail(error, cause, sys._getframe(1))


# condition helpers


def is_null(value) -> IsNull:
    return IsNull(lift(value))


def one_of(value, values) -> OneOf:
    vals = tuple(values)
    for v in vals:
        lift(v)
    return OneOf(lift(value), vals)


def length(value) -> Length:
    return Length(lift(value))


__all__ = [
    "Err",
    "Lit",
    "Ok",
    "S3File",
    "S3Objects",
    "States",
    "attempt",
    "each",
    "fail",
    "is_null",
    "length",
    "one_of",
    "subworkflow",
    "wait",
]
