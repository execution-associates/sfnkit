"""``@step``: a function that runs as one unit of compute (a Lambda function by default)."""

from __future__ import annotations

import functools
import inspect
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from sfnkit.compute.base import ComputeSpec
from sfnkit.errors import TraceError
from sfnkit.expr import Expr, lift
from sfnkit.iam import Policy, Statement
from sfnkit.image import Image

_REGISTRY: dict[str, Step] = {}


def error_name(e: str | type[BaseException]) -> str:
    """Step Functions sees a Python exception as its class name (Lambda's ``errorType``)."""
    if isinstance(e, str):
        return e
    if isinstance(e, type) and issubclass(e, BaseException):
        return e.__name__
    raise TypeError(f"expected an error name or exception class, got {e!r}")


@dataclass(frozen=True)
class Retry:
    """Retry a step on the named errors. ``max_attempts`` counts retries after the first try."""

    on: Sequence[str | type[BaseException]] = ("States.ALL",)
    max_attempts: int = 3
    interval: int = 1
    backoff: float = 2.0
    max_delay: int | None = None
    jitter: bool = True

    def to_asl(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "ErrorEquals": [error_name(e) for e in self.on],
            "IntervalSeconds": max(1, int(self.interval)),
            "MaxAttempts": int(self.max_attempts),
            "BackoffRate": float(self.backoff),
        }
        if self.max_delay is not None:
            out["MaxDelaySeconds"] = int(self.max_delay)
        if self.jitter:
            out["JitterStrategy"] = "FULL"
        return out


def _as_policy(iam) -> Policy | None:
    if iam is None or isinstance(iam, Policy):
        return iam
    if isinstance(iam, Statement):
        return Policy(iam)
    raise TypeError("iam= takes a Policy(...) or an Allow(...)/Deny(...) statement")


def _check_importable(fn: Callable, what: str) -> None:
    qual = getattr(fn, "__qualname__", "")
    if fn.__name__ == "<lambda>" or "<locals>" in qual:
        raise TraceError(
            f"{what} {qual} must be a module-level function so the deployed runtime can import it"
        )


class Step:
    def __init__(
        self,
        fn: Callable,
        *,
        name: str | None = None,
        compute: ComputeSpec | None = None,
        deps: Sequence[str] = (),
        system_packages: Sequence[str] = (),
        image: Image | None = None,
        iam: Policy | Statement | None = None,
        retry: Retry | Sequence[Retry] | None = None,
        env: dict[str, str] | None = None,
    ):
        _check_importable(fn, "@step")
        if image is not None and not isinstance(image, Image):
            raise TypeError("image= takes an sfnkit.Image(...)")
        self.fn = fn
        self.key = f"{fn.__module__}.{fn.__qualname__}"
        self.name = name or fn.__name__
        self.compute = compute
        self.deps = tuple(deps)
        self.system_packages = tuple(system_packages)
        self.image = image
        self.iam = _as_policy(iam)
        self.retry = [retry] if isinstance(retry, Retry) else list(retry or [])
        self.env = dict(env or {})
        self.signature = inspect.signature(fn)
        functools.update_wrapper(self, fn)
        _REGISTRY[self.key] = self

    def bind(self, args: tuple, kwargs: dict) -> dict[str, Expr]:
        try:
            bound = self.signature.bind(*args, **kwargs)
        except TypeError as e:
            raise TraceError(f"bad arguments to step {self.name}: {e}") from None
        bound.apply_defaults()
        out = {}
        for k, v in bound.arguments.items():
            p = self.signature.parameters[k]
            if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
                raise TraceError(f"step {self.name}: *args/**kwargs parameters can't be traced")
            out[k] = lift(v)
        return out

    def __call__(self, *args, **kwargs):
        from sfnkit.trace.tracer import current_tracer

        t = current_tracer()
        if t is None:
            for v in list(args) + list(kwargs.values()):
                if isinstance(v, Expr):
                    raise TraceError(f"step {self.name} got a workflow value outside a workflow trace")
            return self.fn(*args, **kwargs)
        return t.task(self, self.bind(args, kwargs), sys._getframe(1))

    def map(
        self,
        items,
        *,
        max_concurrency: int | None = None,
        distributed: bool = False,
        tolerated_failure_percentage: float | None = None,
        tolerated_failure_count: int | None = None,
        child_type: str = "STANDARD",
        **kwargs,
    ):
        """Run this step once per item (a Map state). ``kwargs`` are passed unchanged to every call;
        the item goes to the first parameter not given in ``kwargs``."""
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

    def item_param(self, kwargs: dict) -> str:
        for name, p in self.signature.parameters.items():
            if name not in kwargs and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY):
                return name
        raise TraceError(f"step {self.name}.map(): no parameter left to receive the item")

    def __repr__(self):
        return f"<step {self.key}>"


def step(fn: Callable | None = None, **options):
    """Mark a function as a workflow step.

    Usable bare (``@step``) or with options: ``compute=Lambda(...)``, ``deps=[...]``,
    ``system_packages=[...]``, ``image=Image(...)``, ``iam=Policy(...)``, ``retry=Retry(...)``, ``env={...}``, ``name=...``.
    """
    if fn is not None:
        return Step(fn, **options)
    return lambda f: Step(f, **options)


def lookup(key: str) -> Step:
    """Find a step by key, importing its module if needed (used by the deployed runtime)."""
    if key not in _REGISTRY:
        module = key.rsplit(".", 1)[0]
        while module and module not in sys.modules:
            try:
                __import__(module)
                break
            except ModuleNotFoundError:
                module = module.rsplit(".", 1)[0] if "." in module else ""
    try:
        return _REGISTRY[key]
    except KeyError:
        raise LookupError(f"no step registered as {key!r}") from None
