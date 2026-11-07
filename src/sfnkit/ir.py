"""The merged workflow graph: scopes of statements, compiled to ASL and run by the local runner.

Values (things an expression can ``Get``): ``InputV``, ``ItemV``, ``PhiV`` and the statements that
produce one (``TaskN``, ``MapN``, ``CallN``, ``AttemptN``). ``SubN`` (an inline child state machine)
produces no value of its own; values created inside it are used directly.

A ``ChoiceN`` has ordered ``rules`` (condition -> branch index), a ``default`` branch, and ``phis``:
values that differ by branch, assigned at the end of each branch.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from sfnkit.expr import Cond, Expr, Get
from sfnkit.iam import Policy


class Ids:
    def __init__(self):
        self.n = 0

    def next(self, prefix: str) -> str:
        self.n += 1
        return f"{prefix}{self.n}"


class Value:
    id: str
    label: str = "value"

    def __repr__(self):
        return f"<{type(self).__name__} {self.id} {self.label}>"


class InputV(Value):
    label = "input"

    def __init__(self, id: str, params: list[str], defaults: dict[str, Any]):
        self.id, self.params, self.defaults = id, params, defaults


class ItemV(Value):
    label = "item"

    def __init__(self, id: str, map: MapN):
        self.id, self.map = id, map


class PhiV(Value):
    label = "phi"

    def __init__(self, id: str, choice: ChoiceN):
        self.id = id
        self.choice = choice
        self.values: dict[int, Expr] = {}


class Stmt:
    id: str
    scope: Scope
    where: str = ""

    def child_scopes(self) -> list[Scope]:
        return []


class TaskN(Stmt, Value):
    def __init__(self, id, scope, step, args: dict[str, Expr], policy: Policy, where=""):
        self.id, self.scope, self.step, self.args, self.policy, self.where = id, scope, step, args, policy, where
        self.label = step.name


class CallN(Stmt, Value):
    def __init__(self, id, scope, workflow, args: dict[str, Expr], where=""):
        self.id, self.scope, self.workflow, self.args, self.where = id, scope, workflow, args, where
        self.label = workflow.name


class MapN(Stmt, Value):
    def __init__(self, id, scope, items, opts: dict, where=""):
        self.id, self.scope, self.items, self.opts, self.where = id, scope, items, opts, where
        self.body: Scope = None  # type: ignore[assignment]
        self.item: ItemV = None  # type: ignore[assignment]
        self.label = "each"

    @property
    def distributed(self) -> bool:
        return bool(self.opts.get("distributed"))

    def child_scopes(self):
        return [self.body]


class AttemptN(Stmt, Value):
    def __init__(self, id, scope, handled: list[str] | None, where=""):
        self.id, self.scope, self.handled, self.where = id, scope, handled, where
        self.body: Scope = None  # type: ignore[assignment]
        self.label = "attempt"

    def child_scopes(self):
        return [self.body]


class SubN(Stmt, Value):
    def __init__(self, id, scope, name: str, iam: Policy | None, type: str, where=""):
        self.id, self.scope, self.name, self.iam, self.type, self.where = id, scope, name, iam, type, where
        self.body: Scope = None  # type: ignore[assignment]
        self.label = name

    def child_scopes(self):
        return [self.body]


class WaitN(Stmt):
    def __init__(self, id, scope, seconds: Expr | None, until: Expr | None, where=""):
        self.id, self.scope, self.seconds, self.until, self.where = id, scope, seconds, until, where


class FailN(Stmt):
    def __init__(self, id, scope, error: Expr, cause: Expr, where=""):
        self.id, self.scope, self.error, self.cause, self.where = id, scope, error, cause, where


class ChoiceN(Stmt):
    def __init__(self, id, scope, where=""):
        self.id, self.scope, self.where = id, scope, where
        self.rules: list[tuple[Cond, int]] = []
        self.branches: list[Scope] = []
        self.default: int = 0
        self.phis: list[PhiV] = []

    def child_scopes(self):
        return list(self.branches)


class Scope:
    def __init__(self, kind: str, parent: Stmt | None):
        self.kind = kind  # root | branch | each | attempt | sub
        self.parent = parent
        self.stmts: list[Stmt] = []
        self.result: Expr | None = None
        self.policy: Policy | None = None  # iam base set declared at this scope (root/sub)

    def ancestors(self) -> Iterator[Scope]:
        s: Scope | None = self
        while s is not None:
            yield s
            s = s.parent.scope if s.parent is not None else None

    def walk(self) -> Iterator[Stmt]:
        """Every statement in this scope and nested scopes, in program order."""
        for st in self.stmts:
            yield st
            for cs in st.child_scopes():
                yield from cs.walk()


class Graph:
    def __init__(self, workflow, root: Scope, input: InputV):
        self.workflow = workflow
        self.root = root
        self.input = input

    def tasks(self) -> list[TaskN]:
        return [s for s in self.root.walk() if isinstance(s, TaskN)]

    def calls(self) -> list[CallN]:
        return [s for s in self.root.walk() if isinstance(s, CallN)]


def gets(e: Expr | None) -> Iterator[Get]:
    """Every Get inside an expression."""
    if e is None:
        return
    found: list[Get] = []

    def f(g):
        found.append(g)
        return g

    e.remap(f)
    yield from found


def dump(g: Graph) -> str:
    """Readable text form of a graph, for debugging and tests."""
    lines: list[str] = []

    def name(v) -> str:
        return getattr(v, "id", type(v).__name__)

    def ex(e) -> str:
        if e is None:
            return "-"
        def f(gt):
            return Get(_Named(name(gt.src)), gt.path)
        return repr(e.remap(f))

    def sc(s: Scope, ind: str) -> None:
        for st in s.stmts:
            if isinstance(st, TaskN):
                lines.append(f"{ind}{st.id} = {st.step.name}({', '.join(f'{k}={ex(v)}' for k, v in st.args.items())})")
            elif isinstance(st, CallN):
                lines.append(f"{ind}{st.id} = call {st.workflow.name}({', '.join(f'{k}={ex(v)}' for k, v in st.args.items())})")
            elif isinstance(st, MapN):
                items = ex(st.items) if isinstance(st.items, Expr) else repr(st.items)
                lines.append(f"{ind}{st.id} = map {items} item={st.item.id} {'distributed' if st.distributed else ''}")
                sc(st.body, ind + "  ")
                lines.append(f"{ind}  -> {ex(st.body.result)}")
            elif isinstance(st, AttemptN):
                lines.append(f"{ind}{st.id} = attempt handled={st.handled}")
                sc(st.body, ind + "  ")
                lines.append(f"{ind}  -> {ex(st.body.result)}")
            elif isinstance(st, SubN):
                lines.append(f"{ind}{st.id}: sub {st.name}")
                sc(st.body, ind + "  ")
            elif isinstance(st, WaitN):
                lines.append(f"{ind}{st.id}: wait {ex(st.seconds)} {ex(st.until)}")
            elif isinstance(st, FailN):
                lines.append(f"{ind}{st.id}: fail {ex(st.error)} {ex(st.cause)}")
            elif isinstance(st, ChoiceN):
                lines.append(f"{ind}{st.id}: choice")
                for cond, b in st.rules:
                    lines.append(f"{ind}  if {ex(cond)} -> b{b}")
                lines.append(f"{ind}  default -> b{st.default}")
                for i, b in enumerate(st.branches):
                    lines.append(f"{ind}  b{i}:")
                    sc(b, ind + "    ")
                for p in st.phis:
                    lines.append(f"{ind}  {p.id} = " + ", ".join(f"b{i}:{ex(v)}" for i, v in sorted(p.values.items())))

    sc(g.root, "")
    lines.append(f"return {ex(g.root.result)}")
    return "\n".join(lines)


class _Named:
    def __init__(self, label: str):
        self.label = label
