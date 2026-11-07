"""Multi-path tracing: run a workflow body once per combination of decisions.

Every ``bool()`` on a condition is a decision. ``explore`` runs the body repeatedly, depth-first over
decision sequences: each run forces a prefix of choices, new decisions default to True, and the deepest
True choice is flipped for the next run. ``merge`` later turns the recorded paths into one graph.

Identity of an event is its call-site stack (code object + instruction offset of every frame between
the workflow function and the call) plus how often that stack was hit in this path, so the same line
reached in two paths is recognised as the same node.
"""

from __future__ import annotations

import contextvars
from collections import Counter
from dataclasses import dataclass
from typing import Any

from sfnkit.errors import TraceError
from sfnkit.expr import Cmp, Cond, Expr, Get, IsOk, Lit, OutcomeGet, lift, strip_not

_current: contextvars.ContextVar[Tracer | None] = contextvars.ContextVar("sfnkit_tracer", default=None)


def current_tracer() -> Tracer | None:
    return _current.get()


def require_tracer(what: str) -> Tracer:
    t = _current.get()
    if t is None:
        raise TraceError(f"{what} can only be used inside a @workflow function")
    return t


class Terminated(BaseException):
    """Raised by ``fail()`` to end the current path. BaseException so user ``except Exception`` can't
    swallow it."""


class InputSrc:
    label = "input"


class ItemSrc:
    label = "item"

    def __init__(self, node: TNode):
        self.node = node


class TScope:
    def __init__(self, kind: str, parent: TScope | None, node: TNode | None = None):
        self.kind = kind  # root | each | attempt | sub
        self.parent = parent
        self.node = node

    def within(self, other: TScope) -> bool:
        s: TScope | None = self
        while s is not None:
            if s is other:
                return True
            s = s.parent
        return False


class TNode:
    def __init__(self, kind: str, identity: tuple, scope: TScope, **kw: Any):
        self.kind = kind  # task | each | call | attempt | sub | wait
        self.identity = identity
        self.scope = scope
        self.step = kw.get("step")
        self.workflow = kw.get("workflow")
        self.args: dict[str, Expr] = kw.get("args", {})
        self.opts: dict[str, Any] = kw.get("opts", {})
        self.items = kw.get("items")
        self.handled = kw.get("handled")
        self.body: TScope | None = None
        self.item: ItemSrc | None = None
        self.result: Expr | None = None
        self.closed = False
        self.where = kw.get("where", "")

    @property
    def label(self) -> str:
        if self.step is not None:
            return self.step.name
        if self.workflow is not None:
            return self.workflow.name
        return self.opts.get("name") or self.kind


@dataclass(eq=False)
class NodeEv:
    identity: tuple
    tnode: TNode


@dataclass(eq=False)
class DecEv:
    identity: tuple
    cond: Cond
    value: bool
    where: str


@dataclass(eq=False)
class EnterEv:
    identity: tuple
    tnode: TNode


@dataclass(eq=False)
class ExitEv:
    identity: tuple
    tnode: TNode
    result: Expr | None


@dataclass(eq=False)
class FailEv:
    identity: tuple
    error: Expr
    cause: Expr


@dataclass(eq=False)
class EndEv:
    identity: tuple
    result: Expr


END = ("END",)


def _where(frame) -> str:
    return f"{frame.f_code.co_filename}:{frame.f_lineno}"


class Tracer:
    def __init__(self, workflow, forced: list[bool]):
        self.workflow = workflow
        self.root_code = workflow.fn.__code__
        self.forced = forced
        self.choices: list[bool] = []
        self.events: list = []
        self.memo: dict[tuple, bool] = {}
        self.counts: Counter = Counter()
        self.input = InputSrc()
        self.root = TScope("root", None)
        self.scope = self.root
        self.domains: dict[int, list[str] | None] = {}  # attempt node id -> handled error names
        self.dec_log: list[str] = []

    # identity
    def site(self, frame) -> tuple:
        stack = []
        f = frame
        while f is not None:
            stack.append((f.f_code, f.f_lasti))
            if f.f_code is self.root_code:
                break
            f = f.f_back
        else:
            raise TraceError("a step was called outside the workflow function's own thread of execution")
        key = tuple(stack)
        n = self.counts[key]
        self.counts[key] += 1
        return (key, n)

    # running
    def run(self) -> None:
        token = _current.set(self)
        try:
            params = {name: Get(self.input, (name,)) for name in self.workflow.signature.parameters}
            try:
                result = self.workflow.fn(**params)
            except Terminated:
                return
            except TraceError as e:
                raise TraceError(f"{e}{self.path_note()}") from None
            except Exception as e:
                raise TraceError(
                    f"tracing {self.workflow.name} raised {type(e).__name__}: {e}{self.path_note()}"
                ) from e
            self.check_visible(lift(result) if not isinstance(result, Expr) else result, self.root)
            self.events.append(EndEv(END, lift(result)))
        finally:
            _current.reset(token)

    def path_note(self) -> str:
        if not self.dec_log:
            return ""
        return "\n  on the path where:\n    " + "\n    ".join(self.dec_log)

    # visibility of values across blocks
    def check_visible(self, e: Expr, scope: TScope | None = None) -> None:
        scope = scope or self.scope
        for src in e.sources():
            if isinstance(src, InputSrc):
                if src is not self.input:
                    raise TraceError("a value from a different workflow trace was used here")
            elif isinstance(src, ItemSrc):
                if not scope.within(src.node.body):
                    raise TraceError("it.item can only be used inside its `with each(...)` block")
            elif isinstance(src, TNode):
                if src.kind in ("each", "attempt") and not src.closed:
                    raise TraceError(f"the result of {src.label} is used before its block finished")
                x = src.scope
                while not scope.within(x):
                    if x.kind == "sub":
                        x = x.parent
                        continue
                    kind = "each" if x.kind == "each" else "attempt"
                    how = "it.result(...)" if kind == "each" else "Ok(value)"
                    raise TraceError(
                        f"{src.label} was created inside an {kind} block and can't be used outside it; "
                        f"pass it out with {how}"
                    )
            else:
                raise TraceError(f"unexpected value source {src!r}")

    # recording
    def task(self, step, args: dict[str, Expr], frame) -> Get:
        ident = self.site(frame)
        for e in args.values():
            self.check_visible(e)
        n = TNode("task", ident, self.scope, step=step, args=args, where=_where(frame))
        self.events.append(NodeEv(ident, n))
        return Get(n)

    def call(self, workflow, args: dict[str, Expr], frame) -> Get:
        if workflow is self.workflow:
            raise TraceError(f"workflow {workflow.name} calls itself")
        ident = self.site(frame)
        for e in args.values():
            self.check_visible(e)
        n = TNode("call", ident, self.scope, workflow=workflow, args=args, where=_where(frame))
        self.events.append(NodeEv(ident, n))
        return Get(n)

    def each_enter(self, items, opts: dict, frame) -> TNode:
        from sfnkit.blocks import S3File, S3Objects

        ident = self.site(frame)
        if not isinstance(items, (S3Objects, S3File)):
            items = lift(items)
            self.check_visible(items)
        elif not opts.get("distributed"):
            raise TraceError("S3Objects/S3File item sources need distributed=True")
        n = TNode("each", ident, self.scope, items=items, opts=opts, where=_where(frame))
        n.body = TScope("each", self.scope, n)
        n.item = ItemSrc(n)
        self.events.append(EnterEv(ident, n))
        self.scope = n.body
        return n

    def each_exit(self, n: TNode, result: Expr | None) -> None:
        if result is not None:
            self.check_visible(result, n.body)
        self.scope = n.body.parent  # type: ignore[union-attr]
        n.closed = True
        self.events.append(ExitEv(n.identity + ("exit",), n, result))

    def sub_enter(self, opts: dict, frame) -> TNode:
        ident = self.site(frame)
        n = TNode("sub", ident, self.scope, opts=opts, where=_where(frame))
        n.body = TScope("sub", self.scope, n)
        self.events.append(EnterEv(ident, n))
        self.scope = n.body
        return n

    def sub_exit(self, n: TNode) -> None:
        self.scope = n.body.parent  # type: ignore[union-attr]
        n.closed = True
        self.events.append(ExitEv(n.identity + ("exit",), n, None))

    def attempt(self, fn, args: tuple, kwargs: dict, frame) -> OutcomeGet:
        from sfnkit.step import Step
        from sfnkit.workflow import Workflow

        handled = self.workflow.lint.attempt_handled(frame)
        ident = self.site(frame)
        n = TNode("attempt", ident, self.scope, handled=handled, where=_where(frame))
        n.body = TScope("attempt", self.scope, n)
        self.events.append(EnterEv(ident, n))
        self.scope = n.body
        if isinstance(fn, Step):
            r = self.task(fn, fn.bind(args, kwargs), frame)
        elif isinstance(fn, Workflow):
            r = self.call(fn, fn.bind(args, kwargs), frame)
        elif callable(fn):
            r = lift(fn(*args, **kwargs))
        else:
            raise TraceError(f"attempt() needs a step, workflow or function, got {fn!r}")
        self.check_visible(r, n.body)
        self.scope = n.body.parent  # type: ignore[union-attr]
        n.closed = True
        n.result = r
        self.events.append(ExitEv(ident + ("exit",), n, r))
        self.domains[id(n)] = handled
        return OutcomeGet(n)

    def wait(self, seconds, until, frame) -> None:
        if (seconds is None) == (until is None):
            raise TraceError("wait() takes exactly one of seconds= or until=")
        ident = self.site(frame)
        opts = {"seconds": None if seconds is None else lift(seconds), "until": None if until is None else lift(until)}
        for v in opts.values():
            if v is not None:
                self.check_visible(v)
        n = TNode("wait", ident, self.scope, opts=opts, where=_where(frame))
        self.events.append(NodeEv(ident, n))

    def fail(self, error, cause, frame) -> None:
        ident = self.site(frame)
        err, cau = lift(error), lift(cause)
        self.check_visible(err)
        self.check_visible(cau)
        self.events.append(FailEv(ident, err, cau))
        raise Terminated()

    # decisions
    def decide(self, cond: Cond, frame) -> bool:
        ident = self.site(frame)
        if ident[1] > 0:
            raise TraceError(
                f"a condition at {_where(frame)} runs more than once in one path (inside a Python loop). "
                "Conditions inside loops can't be traced; use each(...) or move the loop into a step."
            )
        self.check_visible(cond)
        base, neg = strip_not(cond)
        k = base.key()
        if k in self.memo:
            v = self.memo[k]
        else:
            forced = self._constraint(base)
            if forced is not None:
                v = forced
            else:
                i = len(self.choices)
                v = self.forced[i] if i < len(self.forced) else True
                self.choices.append(v)
            self.memo[k] = v
        result = v != neg
        self.events.append(DecEv(("dec",) + ident, cond, result, _where(frame)))
        self.dec_log.append(f"{_where(frame)}: {cond!r} is {result}")
        return result

    def _constraint(self, base: Cond) -> bool | None:
        """Outcomes of attempt() can only fail with the error names their `match` handles."""
        if (
            isinstance(base, Cmp)
            and base.op == "=="
            and isinstance(base.a, Get)
            and base.a.path == ("error",)
            and isinstance(base.a.src, TNode)
            and base.a.src.kind == "attempt"
            and isinstance(base.b, Lit)
        ):
            node = base.a.src
            if self.memo.get(IsOk(Get(node)).key()) is True:
                return False
            handled = self.domains.get(id(node))
            if handled is None:
                return None
            name = base.b.value
            if name not in handled:
                return False
            others = [n for n in handled if n != name]
            if all(self.memo.get(Cmp("==", Get(node, ("error",)), Lit(o)).key()) is False for o in others):
                return True
        return None


def explore(workflow) -> list[Tracer]:
    paths: list[Tracer] = []
    forced: list[bool] = []
    while True:
        t = Tracer(workflow, forced)
        t.run()
        paths.append(t)
        ch = t.choices
        i = len(ch) - 1
        while i >= 0 and not ch[i]:
            i -= 1
        if i < 0:
            return paths
        if len(paths) >= workflow.max_paths:
            raise TraceError(
                f"workflow {workflow.name} has more than {workflow.max_paths} paths through its conditions. "
                "Split independent parts into separate @workflow functions (each is traced on its own) "
                "or raise max_paths."
            )
        forced = ch[:i] + [False]
