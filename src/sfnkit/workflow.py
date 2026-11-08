"""``@workflow``: a function whose body is traced into a Step Functions state machine."""

from __future__ import annotations

import functools
import inspect
import json
import sys
from collections.abc import Callable, Sequence
from typing import Any

from sfnkit.compute.base import ComputeSpec
from sfnkit.errors import TraceError
from sfnkit.expr import Expr, lift
from sfnkit.iam import Policy, Statement
from sfnkit.image import Image
from sfnkit.step import _as_policy, _check_importable
from sfnkit.trace.lint import lint


class Workflow:
    def __init__(
        self,
        fn: Callable,
        *,
        name: str | None = None,
        iam: Policy | Statement | None = None,
        deps: Sequence[str] = (),
        system_packages: Sequence[str] = (),
        image: Image | None = None,
        default_compute: ComputeSpec | None = None,
        type: str = "STANDARD",
        python: str = "3.14",
        architecture: str = "x86_64",
        max_paths: int = 1024,
        exclude: Sequence[str] = (),
        offload_threshold: int = 32 * 1024,
    ):
        _check_importable(fn, "@workflow")
        if type not in ("STANDARD", "EXPRESS"):
            raise ValueError("type must be STANDARD or EXPRESS")
        if architecture not in ("x86_64", "arm64"):
            raise ValueError("architecture must be x86_64 or arm64")
        if image is not None and not isinstance(image, Image):
            raise TypeError("image= takes an sfnkit.Image(...)")
        self.fn = fn
        self.name = name or fn.__name__
        self.module = fn.__module__
        self.key = f"{fn.__module__}.{fn.__qualname__}"
        self.iam = _as_policy(iam)
        self.deps = tuple(deps)
        self.system_packages = tuple(system_packages)
        self.image = image
        self.default_compute = default_compute
        self.type = type
        self.python = python
        self.architecture = architecture
        self.max_paths = max_paths
        self.exclude = tuple(exclude)
        self.offload_threshold = offload_threshold
        self.signature = inspect.signature(fn)
        self.defaults: dict[str, Any] = {}
        for pname, p in self.signature.parameters.items():
            if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
                raise TraceError(f"workflow {self.name}: *args/**kwargs parameters aren't supported")
            if p.default is not p.empty:
                try:
                    json.dumps(p.default)
                except (TypeError, ValueError):
                    raise TraceError(f"workflow {self.name}: default for {pname} isn't JSON-serializable") from None
                self.defaults[pname] = p.default
        self.lint = lint(fn)
        self._graph = None
        functools.update_wrapper(self, fn)

    def bind(self, args: tuple, kwargs: dict) -> dict[str, Expr]:
        try:
            bound = self.signature.bind(*args, **kwargs)
        except TypeError as e:
            raise TraceError(f"bad arguments to workflow {self.name}: {e}") from None
        bound.apply_defaults()
        return {k: lift(v) for k, v in bound.arguments.items()}

    def item_param(self, kwargs: dict) -> str:
        for pname in self.signature.parameters:
            if pname not in kwargs:
                return pname
        raise TraceError(f"workflow {self.name}.map(): no parameter left to receive the item")

    def __call__(self, *args, **kwargs):
        from sfnkit.trace.tracer import current_tracer

        t = current_tracer()
        if t is None:
            return self.local(*args, **kwargs)
        return t.call(self, self.bind(args, kwargs), sys._getframe(1))

    def map(self, items, *, max_concurrency=None, distributed=False, tolerated_failure_percentage=None,
            tolerated_failure_count=None, child_type="STANDARD", **kwargs):
        from sfnkit.blocks import _map_call

        return _map_call(
            self,
            items,
            kwargs,
            dict(
                max_concurrency=max_concurrency,
                distributed=distributed,
                tolerated_failure_percentage=tolerated_failure_percentage,
                tolerated_failure_count=tolerated_failure_count,
                child_type=child_type,
            ),
            sys._getframe(1),
        )

    def graph(self):
        """The merged graph of every path through the body (traced once, then cached)."""
        if self._graph is None:
            from sfnkit.trace.merge import merge
            from sfnkit.trace.tracer import explore

            self._graph = merge(self, explore(self))
        return self._graph

    def local(self, *args, **kwargs):
        """Run the workflow in-process from its merged graph, exactly as it would deploy."""
        from sfnkit.local import run_local

        try:
            bound = self.signature.bind(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f"workflow {self.name}: {e}") from None
        bound.apply_defaults()
        return run_local(self, dict(bound.arguments))

    def to_asl(self) -> dict:
        from sfnkit.compile.app import build_app

        return build_app(self).root.definition

    def __repr__(self):
        return f"<workflow {self.key}>"


def workflow(fn: Callable | None = None, **options):
    """Mark a function as a workflow. Options: ``iam=``, ``deps=``, ``system_packages=``, ``image=`` (the
    default image for its steps), ``default_compute=``, ``type="STANDARD"|"EXPRESS"``, ``python=``, ``architecture=``, ``max_paths=``,
    ``exclude=``, ``offload_threshold=``, ``name=``."""
    if fn is not None:
        return Workflow(fn, **options)
    return lambda f: Workflow(f, **options)
