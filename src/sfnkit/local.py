"""Run a workflow in-process from its merged graph, so a local run exercises what deploys.

Values take a JSON round trip at every step boundary, like they do in Step Functions. ``attempt``
catches by exception class name, as Lambda reports errors. Waits are skipped unless ``real_waits``.
"""

from __future__ import annotations

import csv
import io
import json
import math
import time
import traceback
from typing import Any

from sfnkit.errors import SfnkitError, ToleratedFailureExceeded
from sfnkit.expr import (
    And,
    Cmp,
    Contains,
    Expr,
    Get,
    IsNull,
    IsOk,
    Length,
    Lit,
    Not,
    OneOf,
    Or,
    Struct,
    Truthy,
)
from sfnkit.ir import AttemptN, CallN, ChoiceN, FailN, MapN, Scope, SubN, TaskN, WaitN


class WorkflowFailed(SfnkitError):
    """A ``fail(error, cause)`` state was reached (or a step error went unhandled in a child)."""

    def __init__(self, error: str, cause: Any = None):
        super().__init__(f"{error}: {cause}" if cause else error)
        self.error, self.cause = error, cause


def _rt(v: Any) -> Any:
    return json.loads(json.dumps(v))


def run_local(workflow, inputs: dict[str, Any], *, real_waits: bool = False, s3=None) -> Any:
    g = workflow.graph()
    args = {**g.input.defaults, **_rt(inputs)}
    missing = [p for p in g.input.params if p not in args]
    if missing:
        raise TypeError(f"workflow {workflow.name} is missing inputs: {', '.join(missing)}")
    runner = _Runner(real_waits, s3)
    env: dict[int, Any] = {id(g.input): args}
    return _rt(runner.scope(g.root, env))


def tolerated(opts: dict, n: int) -> int | None:
    """How many of ``n`` items may fail (None: none, the first failure fails the map). Either limit
    being passed fails the map, as in a Distributed Map."""
    limits = []
    if opts.get("tolerated_failure_count") is not None:
        limits.append(int(opts["tolerated_failure_count"]))
    if opts.get("tolerated_failure_percentage") is not None:
        limits.append(math.floor(n * float(opts["tolerated_failure_percentage"]) / 100))
    return min(limits) if limits else None


def index(v: Any, path: tuple) -> Any:
    """Project like JSONata does: a missing key or index gives None instead of an error."""
    for p in path:
        if isinstance(v, dict):
            v = v.get(p) if isinstance(p, str) else None
        elif isinstance(v, list) and isinstance(p, int):
            v = v[p] if -len(v) <= p < len(v) else None
        else:
            return None
    return v


class _Runner:
    def __init__(self, real_waits: bool, s3):
        self.real_waits = real_waits
        self._s3 = s3

    def scope(self, scope: Scope, env: dict) -> Any:
        for st in scope.stmts:
            self.stmt(st, env)
        return None if scope.result is None else self.eval(scope.result, env)

    def stmt(self, st, env: dict) -> None:
        if isinstance(st, TaskN):
            args = {k: _rt(self.eval(e, env)) for k, e in st.args.items()}
            env[id(st)] = _rt(st.step.fn(**args))
        elif isinstance(st, CallN):
            args = {k: self.eval(e, env) for k, e in st.args.items()}
            env[id(st)] = run_local(st.workflow, args, real_waits=self.real_waits, s3=self._s3)
        elif isinstance(st, MapN):
            env[id(st)] = self.map(st, env)
        elif isinstance(st, SubN):
            self.scope(st.body, env)
        elif isinstance(st, AttemptN):
            env[id(st)] = self.attempt(st, env)
        elif isinstance(st, WaitN):
            if self.real_waits and st.seconds is not None:
                time.sleep(float(self.eval(st.seconds, env)))
        elif isinstance(st, FailN):
            raise WorkflowFailed(str(self.eval(st.error, env)), self.eval(st.cause, env))
        elif isinstance(st, ChoiceN):
            branch = st.default
            for cond, b in st.rules:
                if self.cond(cond, env):
                    branch = b
                    break
            self.scope(st.branches[branch], env)
            # Step Functions evaluates a whole Assign against the state on entry, so compute first.
            vals = {id(phi): self.eval(phi.values[branch], env) for phi in st.phis}
            env.update(vals)
        else:
            raise TypeError(f"unknown statement {st!r}")

    def map(self, st: MapN, env: dict) -> list:
        items = self.items(st, env)
        if not isinstance(items, list):
            raise TypeError(f"each(...) items must be a list, got {type(items).__name__}")
        allowed = tolerated(st.opts, len(items))
        out = []
        failed: list[BaseException] = []
        for item in items:
            e2 = dict(env)
            e2[id(st.item)] = _rt(item)
            try:
                out.append(self.scope(st.body, e2))
            except Exception as e:
                if allowed is None:
                    raise
                failed.append(e)
                out.append(None)
        if allowed is not None and len(failed) > allowed:
            first = failed[0]
            msg = f"more than {allowed} of {len(items)} items failed; first: {type(first).__name__}: {first}"
            if st.distributed:
                raise WorkflowFailed("States.ExceedToleratedFailureThreshold", msg)
            raise ToleratedFailureExceeded(msg) from first
        return out

    def items(self, st: MapN, env: dict):
        from sfnkit.blocks import S3File, S3Objects

        src = st.items
        if isinstance(src, Expr):
            return self.eval(src, env)
        s3 = self._s3 or __import__("boto3").client("s3")
        if isinstance(src, S3Objects):
            out = []
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=src.bucket, Prefix=src.prefix):
                for o in page.get("Contents", []):
                    out.append(
                        {"Key": o["Key"], "Size": o["Size"], "ETag": o.get("ETag"), "LastModified": str(o["LastModified"])}
                    )
            return out
        if isinstance(src, S3File):
            body = s3.get_object(Bucket=src.bucket, Key=src.key)["Body"].read().decode()
            if src.format == "json":
                return json.loads(body)
            if src.format == "jsonl":
                return [json.loads(line) for line in body.splitlines() if line.strip()]
            return list(csv.DictReader(io.StringIO(body)))
        raise TypeError(f"unknown item source {src!r}")

    def attempt(self, st: AttemptN, env: dict) -> dict:
        try:
            v = self.scope(st.body, dict(env))
            return {"ok": True, "v": v, "error": None, "cause": None}
        except WorkflowFailed as e:
            name, cause = e.error, e.cause
            exc: BaseException = e
        except Exception as e:  # noqa: BLE001 - mirrors Step Functions catching any task error
            name = type(e).__name__
            cause = {
                "errorMessage": str(e),
                "errorType": name,
                "stackTrace": traceback.format_exception(e)[-5:],
            }
            exc = e
        if st.handled is None or name in st.handled:
            return {"ok": False, "v": None, "error": name, "cause": _rt(cause)}
        raise exc

    def eval(self, e: Expr, env: dict) -> Any:
        if isinstance(e, Lit):
            return e.value
        if isinstance(e, Get):
            return index(env[id(e.src)], e.path)
        if isinstance(e, Struct):
            def build(v):
                if isinstance(v, Expr):
                    return self.eval(v, env)
                if isinstance(v, dict):
                    return {k: build(x) for k, x in v.items()}
                return [build(x) for x in v]

            return build(e.value)
        if isinstance(e, Length):
            v = self.eval(e.arg, env)
            return len(v) if v is not None else 0
        return self.cond(e, env)

    def cond(self, c: Expr, env: dict) -> bool:
        if isinstance(c, Cmp):
            a, b = self.eval(c.a, env), self.eval(c.b, env)
            if c.op == "==":
                return a == b
            if c.op == "!=":
                return a != b
            if a is None or b is None:
                return False
            return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[c.op]
        if isinstance(c, And):
            return self.cond(c.a, env) and self.cond(c.b, env)
        if isinstance(c, Or):
            return self.cond(c.a, env) or self.cond(c.b, env)
        if isinstance(c, Not):
            return not self.cond(c.a, env)
        if isinstance(c, IsNull):
            return self.eval(c.a, env) is None
        if isinstance(c, Truthy):
            return bool(self.eval(c.a, env))
        if isinstance(c, Contains):
            cont = self.eval(c.container, env)
            return cont is not None and self.eval(c.item, env) in cont
        if isinstance(c, OneOf):
            return self.eval(c.a, env) in c.values
        if isinstance(c, IsOk):
            return bool(self.eval(c.o, env)["ok"])
        if isinstance(c, Get):
            return bool(self.eval(c, env))
        raise TypeError(f"unknown condition {c!r}")
