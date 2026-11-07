"""Symbolic values inside a workflow body: refs to outputs, projections and conditions.

During tracing a step call returns a ``Get`` (a "ref"). Comparing refs builds ``Cond`` objects, and
asking Python for a condition's truth value (``if``, ``match``, ``and``/``or``) becomes a *decision*
the tracer records. The same classes are reused after merging with IR values as sources.

Sources are any objects: trace nodes while tracing, IR values after merging. ``key()`` identifies an
expression structurally with sources compared by identity.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Iterator
from typing import Any

from sfnkit.errors import TraceError

_OPS = {"==", "!=", "<", "<=", ">", ">="}


def _tracer():
    from sfnkit.trace.tracer import current_tracer

    return current_tracer()


def _decide(cond: Cond) -> bool:
    t = _tracer()
    if t is None:
        raise TraceError(
            f"can't take the truth value of {cond!r} outside a workflow trace. Conditions on step "
            "outputs only work inside a @workflow function."
        )
    # frame 0: _decide, 1: the dunder (__bool__/__contains__/__instancecheck__), 2: user code
    return t.decide(cond, sys._getframe(2))


class Expr:
    __slots__ = ()

    # structural identity, sources by id()
    def key(self) -> tuple:
        raise NotImplementedError

    def sources(self) -> Iterator[Any]:
        return iter(())

    def remap(self, f: Callable[[Get], Expr]) -> Expr:
        """Rebuild with every ``Get`` replaced by ``f(get)``."""
        raise NotImplementedError

    # comparisons build conditions
    def __eq__(self, other):  # type: ignore[override]
        return Cmp("==", self, lift(other))

    def __ne__(self, other):  # type: ignore[override]
        return Cmp("!=", self, lift(other))

    def __lt__(self, other):
        return Cmp("<", self, lift(other))

    def __le__(self, other):
        return Cmp("<=", self, lift(other))

    def __gt__(self, other):
        return Cmp(">", self, lift(other))

    def __ge__(self, other):
        return Cmp(">=", self, lift(other))

    def __and__(self, other):
        return And(as_cond(self), as_cond(lift(other)))

    def __rand__(self, other):
        return And(as_cond(lift(other)), as_cond(self))

    def __or__(self, other):
        return Or(as_cond(self), as_cond(lift(other)))

    def __ror__(self, other):
        return Or(as_cond(lift(other)), as_cond(self))

    def __invert__(self):
        return Not(as_cond(self))

    __hash__ = object.__hash__

    def __bool__(self):
        return _decide(as_cond(self))

    def __contains__(self, item):
        return _decide(Contains(self, lift(item)))

    def __iter__(self):
        raise TraceError(
            "can't iterate over a workflow value while tracing; use step.map(items) or "
            "`with each(items) as it:` to run work per item"
        )

    def __len__(self):
        raise TraceError("len() can't be traced; use sfnkit.length(x) in conditions")

    def __index__(self):
        raise TraceError("a workflow value can't be used as a Python index while tracing")

    def _no_math(self, *_):
        raise TraceError(
            "arithmetic on workflow values can't be traced (the value only exists when the workflow runs); "
            "do the calculation inside a step"
        )

    __add__ = __radd__ = __sub__ = __rsub__ = __mul__ = __rmul__ = _no_math
    __truediv__ = __rtruediv__ = __floordiv__ = __rfloordiv__ = __mod__ = __rmod__ = _no_math
    __pow__ = __rpow__ = __neg__ = __pos__ = __abs__ = _no_math


class Lit(Expr):
    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def key(self):
        return ("lit", json.dumps(self.value, sort_keys=True))

    def remap(self, f):
        return self

    def __repr__(self):
        return repr(self.value)


class Get(Expr):
    """A ref: the value of ``src`` (an output, input, item, phi...), projected along ``path``."""

    __slots__ = ("path", "src")

    def __init__(self, src: Any, path: tuple = ()):
        self.src = src
        self.path = tuple(path)

    def __getitem__(self, k):
        if isinstance(k, bool) or not isinstance(k, (str, int)):
            raise TraceError(f"workflow values can only be indexed by str or int, not {k!r}")
        return Get(self.src, self.path + (k,))

    def key(self):
        return ("get", id(self.src), self.path)

    def sources(self):
        yield self.src

    def remap(self, f):
        return f(self)

    def __repr__(self):
        name = getattr(self.src, "label", None) or type(self.src).__name__
        return name + "".join(f"[{p!r}]" for p in self.path)


class OutcomeGet(Get):
    """The outcome of ``attempt(...)``: match it with ``Ok(value)`` / ``Err(error, cause)``."""

    __slots__ = ()

    @property
    def value(self) -> Get:
        return Get(self.src, ("v",))

    @property
    def error(self) -> Get:
        return Get(self.src, ("error",))

    @property
    def cause(self) -> Get:
        return Get(self.src, ("cause",))

    def remap(self, f):
        return f(self)


class Length(Expr):
    __slots__ = ("arg",)

    def __init__(self, arg: Expr):
        self.arg = arg

    def key(self):
        return ("len", self.arg.key())

    def sources(self):
        return self.arg.sources()

    def remap(self, f):
        return Length(self.arg.remap(f))

    def __repr__(self):
        return f"length({self.arg!r})"


class Cond(Expr):
    __slots__ = ()

    def __bool__(self):
        return _decide(self)


class Cmp(Cond):
    __slots__ = ("a", "b", "op")

    def __init__(self, op: str, a: Expr, b: Expr):
        assert op in _OPS
        self.op, self.a, self.b = op, a, b

    def key(self):
        return ("cmp", self.op, self.a.key(), self.b.key())

    def sources(self):
        yield from self.a.sources()
        yield from self.b.sources()

    def remap(self, f):
        return Cmp(self.op, self.a.remap(f), self.b.remap(f))

    def __repr__(self):
        return f"{self.a!r} {self.op} {self.b!r}"


class And(Cond):
    __slots__ = ("a", "b")

    def __init__(self, a: Cond, b: Cond):
        self.a, self.b = a, b

    def key(self):
        return ("and", self.a.key(), self.b.key())

    def sources(self):
        yield from self.a.sources()
        yield from self.b.sources()

    def remap(self, f):
        return And(self.a.remap(f), self.b.remap(f))

    def __repr__(self):
        return f"({self.a!r} & {self.b!r})"


class Or(Cond):
    __slots__ = ("a", "b")

    def __init__(self, a: Cond, b: Cond):
        self.a, self.b = a, b

    def key(self):
        return ("or", self.a.key(), self.b.key())

    def sources(self):
        yield from self.a.sources()
        yield from self.b.sources()

    def remap(self, f):
        return Or(self.a.remap(f), self.b.remap(f))

    def __repr__(self):
        return f"({self.a!r} | {self.b!r})"


class Not(Cond):
    __slots__ = ("a",)

    def __init__(self, a: Cond):
        self.a = a

    def key(self):
        return ("not", self.a.key())

    def sources(self):
        return self.a.sources()

    def remap(self, f):
        return Not(self.a.remap(f))

    def __repr__(self):
        return f"~{self.a!r}"


class IsNull(Cond):
    __slots__ = ("a",)

    def __init__(self, a: Expr):
        self.a = a

    def key(self):
        return ("isnull", self.a.key())

    def sources(self):
        return self.a.sources()

    def remap(self, f):
        return IsNull(self.a.remap(f))

    def __repr__(self):
        return f"is_null({self.a!r})"


class Truthy(Cond):
    __slots__ = ("a",)

    def __init__(self, a: Expr):
        self.a = a

    def key(self):
        return ("truthy", self.a.key())

    def sources(self):
        return self.a.sources()

    def remap(self, f):
        return Truthy(self.a.remap(f))

    def __repr__(self):
        return f"bool({self.a!r})"


class Contains(Cond):
    """``item in container``."""

    __slots__ = ("container", "item")

    def __init__(self, container: Expr, item: Expr):
        self.container, self.item = container, item

    def key(self):
        return ("in", self.container.key(), self.item.key())

    def sources(self):
        yield from self.container.sources()
        yield from self.item.sources()

    def remap(self, f):
        return Contains(self.container.remap(f), self.item.remap(f))

    def __repr__(self):
        return f"{self.item!r} in {self.container!r}"


class OneOf(Cond):
    __slots__ = ("a", "values")

    def __init__(self, a: Expr, values: tuple):
        self.a, self.values = a, tuple(values)

    def key(self):
        return ("oneof", self.a.key(), json.dumps(self.values))

    def sources(self):
        return self.a.sources()

    def remap(self, f):
        return OneOf(self.a.remap(f), self.values)

    def __repr__(self):
        return f"one_of({self.a!r}, {list(self.values)!r})"


class IsOk(Cond):
    """Whether an ``attempt(...)`` outcome succeeded. ``Err`` is ``Not(IsOk)``."""

    __slots__ = ("o",)

    def __init__(self, o: Get):
        self.o = o

    def key(self):
        return ("isok", self.o.key())

    def sources(self):
        return self.o.sources()

    def remap(self, f):
        return IsOk(self.o.remap(f))

    def __repr__(self):
        return f"is_ok({self.o!r})"


def as_cond(e: Expr) -> Cond:
    return e if isinstance(e, Cond) else Truthy(e)


def lift(value: Any) -> Expr:
    """Turn a Python value into an expression. Exception classes become their names."""
    if isinstance(value, Expr):
        return value
    if isinstance(value, type) and issubclass(value, BaseException):
        return Lit(value.__name__)
    if isinstance(value, dict):
        if any(isinstance(v, Expr) for v in _walk(value)):
            return Struct(value)
    if isinstance(value, (list, tuple)):
        if any(isinstance(v, Expr) for v in _walk(value)):
            return Struct(list(value))
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        raise TraceError(
            f"{value!r} can't be passed through a workflow: values must be JSON-serializable"
        ) from None
    return Lit(value)


def _walk(v):
    if isinstance(v, dict):
        for x in v.values():
            yield from _walk(x)
    elif isinstance(v, (list, tuple)):
        for x in v:
            yield from _walk(x)
    else:
        yield v


class Struct(Expr):
    """A dict/list literal with refs inside, e.g. ``notify({"error": error, "cause": cause})``."""

    __slots__ = ("value",)

    def __init__(self, value: Any):
        def conv(v):
            if isinstance(v, Expr):
                return v
            if isinstance(v, dict):
                if not all(isinstance(k, str) for k in v):
                    raise TraceError("dict keys in workflow values must be strings")
                return {k: conv(x) for k, x in v.items()}
            if isinstance(v, (list, tuple)):
                return [conv(x) for x in v]
            return lift(v)

        self.value = conv(value)

    def key(self):
        def k(v):
            if isinstance(v, Expr):
                return v.key()
            if isinstance(v, dict):
                return ("d",) + tuple((kk, k(x)) for kk, x in sorted(v.items()))
            return ("l",) + tuple(k(x) for x in v)

        return ("struct", k(self.value))

    def sources(self):
        def s(v):
            if isinstance(v, Expr):
                yield from v.sources()
            elif isinstance(v, dict):
                for x in v.values():
                    yield from s(x)
            else:
                for x in v:
                    yield from s(x)

        return s(self.value)

    def remap(self, f):
        def r(v):
            if isinstance(v, Expr):
                return v.remap(f)
            if isinstance(v, dict):
                return {k: r(x) for k, x in v.items()}
            return [r(x) for x in v]

        out = Struct.__new__(Struct)
        out.value = r(self.value)
        return out

    def __repr__(self):
        return repr(self.value)


def strip_not(c: Cond) -> tuple[Cond, bool]:
    neg = False
    while isinstance(c, Not):
        c, neg = c.a, not neg
    return c, neg


def same(a: Expr, b: Expr) -> bool:
    return a.key() == b.key()
