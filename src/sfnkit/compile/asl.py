"""Compile a merged graph into Amazon States Language (JSONata query language).

Data moves through workflow variables. Every value has a *location* in the current variable context:
a JSONata expression plus what the content looks like (``Loc.kind``):

  raw       plain JSON (workflow input, S3 listing items)
  wrapper   a step's result wrapper (``runtime/protocol.py``); facts at ``.f.<key>``
  carrier   ``{"$sw":1,"x":value,"f":{...}}`` built by the ASL (phis, Ok values, child results)
  mapout    ``{"$sw":1,"v":[...]}``, an inline Map's results
  dmapout   ``{"$sw":1,"dmap":...}``, a Distributed Map's ResultWriter manifest
  outcome   ``{"ok":bool,"v":...,"error":str,"cause":{"$sw":1,"c":...}}`` from attempt

Conditions run in JSONata and can't read S3, so every value a condition reads is a *fact*: the step
that produces it computes it (``facts`` in the envelope), or the carrier that copies it computes it in
JSONata. Facts are registered while compiling and rendered at the end, re-rendering until no new
registrations appear.

Scoping rules handled here: Parallel branches and Map iterations get their own variable prefix (an
inner scope may not reuse an outer variable's name). Values leave a Parallel branch through its
``Output`` and are re-assigned outside. Map iterations receive every outer value they use through
``ItemSelector``, which is also what Distributed Map requires.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from sfnkit.compute.base import PAYLOAD, backend_for
from sfnkit.errors import CompileError
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
from sfnkit.ir import (
    AttemptN,
    CallN,
    ChoiceN,
    FailN,
    Graph,
    InputV,
    MapN,
    Scope,
    Stmt,
    SubN,
    TaskN,
    WaitN,
)
from sfnkit.trace.merge import stmt_exprs

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# JSONata text helpers


def jlit(v: Any) -> str:
    return json.dumps(v)


def jpath(path: tuple) -> str:
    out = ""
    for p in path:
        if isinstance(p, int):
            out += f"[{p}]"
        elif _IDENT.match(p):
            out += f".{p}"
        else:
            out += ".`" + p.replace("`", "") + "`"
    return out


def jobj(fields: dict[str, str]) -> str:
    return "{" + ", ".join(f"{jlit(k)}: {v}" for k, v in fields.items()) + "}"


class J:
    """A JSONata value rendered at the end (so facts registered later are included)."""

    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[], str] | str):
        self.fn = fn if callable(fn) else (lambda s=fn: s)

    def render(self) -> str:
        return "{% " + self.fn() + " %}"


# fact registries


class FactReg:
    """Paths into one value that conditions read. Keys are stable hashes of the path."""

    changes = 0

    def __init__(self):
        self.paths: dict[tuple, str] = {}

    def key(self, path: tuple) -> str:
        path = tuple(path)
        k = self.paths.get(path)
        if k is None:
            k = "k_" + hashlib.sha1(json.dumps(path).encode()).hexdigest()[:10]
            self.paths[path] = k
            FactReg.changes += 1
        return k

    def spec(self) -> dict[str, list]:
        return {k: list(p) for p, k in sorted(self.paths.items(), key=lambda kv: kv[1])}

    def __bool__(self):
        return bool(self.paths)


class ItemsReg:
    """List facets a step must return for Map states: (path, mode) -> key + element facts."""

    def __init__(self):
        self.facets: dict[tuple, tuple[str, FactReg]] = {}

    def facet(self, path: tuple, mode: str) -> tuple[str, FactReg]:
        k = (tuple(path), mode)
        if k not in self.facets:
            key = "k_" + hashlib.sha1(json.dumps([list(path), mode]).encode()).hexdigest()[:10]
            self.facets[k] = (key, FactReg())
            FactReg.changes += 1
        return self.facets[k]

    def spec(self) -> dict[str, dict]:
        return {
            key: {"path": list(path), "mode": mode, "facts": reg.spec()}
            for (path, mode), (key, reg) in self.facets.items()
        }


@dataclass
class Loc:
    expr: str
    kind: str
    reg: FactReg | None = None
    node: Any = None  # TaskN for wrapper (items facets); MapN for mapout
    vloc: Loc | None = None  # outcome: the kind/reg of its "v"

    def at(self, expr: str) -> Loc:
        return Loc(expr, self.kind, self.reg, self.node, self.vloc)


class Ctx:
    """A variable context. ``prefix`` keeps names unique across nested scopes."""

    def __init__(self, prefix: str, parent: Ctx | None = None):
        self.prefix = prefix
        self.syms: dict[int, Loc] = dict(parent.syms) if parent else {}

    def var(self, vid: str) -> str:
        return f"{self.prefix}{vid}"

    def loc(self, v) -> Loc:
        try:
            return self.syms[id(v)]
        except KeyError:
            raise CompileError(f"internal: {getattr(v, 'id', v)!r} isn't available here") from None


# fragments


class Frag:
    def __init__(self, states: dict[str, dict], entry: str, exits: list[Callable[[str], None]]):
        self.states, self.entry, self.exits = states, entry, exits

    @staticmethod
    def single(name: str, state: dict) -> Frag:
        return Frag({name: state}, name, [lambda nxt, s=state: s.__setitem__("Next", nxt)])


def seq(frags: list[Frag | None]) -> Frag | None:
    frags = [f for f in frags if f is not None]
    if not frags:
        return None
    states: dict[str, dict] = {}
    for f in frags:
        states.update(f.states)
    for a, b in zip(frags, frags[1:]):
        for ex in a.exits:
            ex(b.entry)
    return Frag(states, frags[0].entry, frags[-1].exits)


def close(frag: Frag | None, final_name: str, final_state: dict) -> tuple[str, dict[str, dict]]:
    """Append a terminal state and return (StartAt, States)."""
    done = Frag.single(final_name, final_state)
    f = seq([frag, done])
    assert f is not None
    final_state.pop("Next", None)
    final_state["End"] = True
    return f.entry, f.states


# the compiler


@dataclass
class Machine:
    """One state machine being compiled (root, reusable child, or inline subworkflow)."""

    name: str
    kind: str  # root | call | sub
    definition: dict = field(default_factory=dict)
    substitutions: dict = field(default_factory=dict)
    functions: list = field(default_factory=list)  # FunctionSpecs invoked directly
    children: list = field(default_factory=list)  # names of child machines started
    map_run: bool = False
    s3_reads: list = field(default_factory=list)  # (bucket, prefix)
    type: str = "STANDARD"


class Compiler:
    """Compiles graphs into machines. ``function_for(task)`` and ``machine_name_for(workflow)`` come
    from the app builder, which owns FunctionSpecs and child-machine naming."""

    def __init__(self, function_for, child_machine_for, sub_name_for):
        self.function_for = function_for
        self.child_machine_for = child_machine_for
        self.sub_name_for = sub_name_for
        self.machines: list[Machine] = []
        self.task_regs: dict[int, FactReg] = {}
        self.items_regs: dict[int, ItemsReg] = {}
        self.phi_regs: dict[int, FactReg] = {}
        self.ok_regs: dict[int, FactReg] = {}
        self.elem_regs: dict[int, FactReg] = {}  # MapN -> facts on its elements (body result carriers)
        self.result_regs: dict[int, FactReg] = {}  # workflow (call child) -> facts on its result
        self.input_regs: dict[tuple, FactReg] = {}  # (workflow, param) -> facts in child mode
        self.names: set[str] = set()
        self.pids = 0

    # registries
    def treg(self, n: TaskN) -> FactReg:
        return self.task_regs.setdefault(id(n), FactReg())

    def ireg(self, n: TaskN) -> ItemsReg:
        return self.items_regs.setdefault(id(n), ItemsReg())

    def state_name(self, label: str, vid: str) -> str:
        base = re.sub(r"[^A-Za-z0-9_\-]", "_", label)[:60] or "state"
        name = f"{base}_{vid}"
        while name in self.names:
            name += "_"
        self.names.add(name)
        return name

    # use analysis (for lifting values out of Parallel branches and subworkflows)
    def analyse(self, root: Scope) -> None:
        self.users: dict[int, list[Stmt | None]] = {}

        def note(e, at: Stmt | None):
            if e is None:
                return
            for g in _gets(e):
                self.users.setdefault(id(g.src), []).append(at)

        def walk(s: Scope):
            owner = s.parent
            for st in s.stmts:
                for e in stmt_exprs(st):
                    note(e, st)
                if isinstance(st, ChoiceN):
                    for p in st.phis:
                        for v in p.values.values():
                            note(v, st)
                for cs in st.child_scopes():
                    walk(cs)
            note(s.result, owner)

        walk(root)

    @staticmethod
    def inside(t: Stmt | None, s: Stmt) -> bool:
        while t is not None:
            if t is s:
                return True
            t = t.scope.parent
        return False

    def used_outside(self, v, s: Stmt) -> bool:
        return any(not self.inside(u, s) for u in self.users.get(id(v), []))

    def defined(self, st: Stmt) -> list:
        """Values compiling ``st`` makes available in the current variable context."""
        if isinstance(st, (TaskN, MapN, CallN, AttemptN)):
            return [st]
        if isinstance(st, SubN):
            return [v for v in self.sub_defined(st.body) if self.used_outside(v, st)]
        if isinstance(st, ChoiceN):
            out: list = list(st.phis)
            for b in st.branches:
                for x in b.stmts:
                    out.extend(self.defined(x))
            return out
        return []

    def sub_defined(self, s: Scope) -> list:
        out: list = []
        for st in s.stmts:
            out.extend(self.defined(st))
        return out

    def refs_deep(self, st: Stmt) -> set[int]:
        out: set[int] = set()

        def note(e):
            if e is not None:
                for g in _gets(e):
                    out.add(id(g.src))

        def walk(s: Scope):
            for x in s.stmts:
                for e in stmt_exprs(x):
                    note(e)
                if isinstance(x, ChoiceN):
                    for p in x.phis:
                        for v in p.values.values():
                            note(v)
                for cs in x.child_scopes():
                    walk(cs)
            note(s.result)

        for e in stmt_exprs(st):
            note(e)
        if isinstance(st, ChoiceN):
            for p in st.phis:
                for v in p.values.values():
                    note(v)
        for cs in st.child_scopes():
            walk(cs)
        return out

    def free_values(self, scope: Scope, graph_values: dict[int, Any]) -> list:
        """Values used inside ``scope`` but defined outside it, in a stable order."""
        inner: set[int] = set()
        for st in scope.walk():
            inner.add(id(st))
            if isinstance(st, ChoiceN):
                inner.update(id(p) for p in st.phis)
            if isinstance(st, MapN):
                inner.add(id(st.item))
        if isinstance(scope.parent, MapN):
            inner.add(id(scope.parent.item))
        seen: dict[int, Any] = {}

        def note(e):
            if e is not None:
                for g in _gets(e):
                    if id(g.src) not in inner and id(g.src) not in seen:
                        seen[id(g.src)] = g.src

        for st in scope.walk():
            for e in stmt_exprs(st):
                note(e)
            if isinstance(st, ChoiceN):
                for p in st.phis:
                    for v in p.values.values():
                        note(v)
            for cs in st.child_scopes():
                note(cs.result)
        note(scope.result)
        return list(seen.values())

    def is_barrier(self, st: Stmt) -> bool:
        if isinstance(st, (WaitN, FailN)):
            return True
        if isinstance(st, ChoiceN):
            return any(isinstance(x, FailN) or (isinstance(x, ChoiceN) and self.is_barrier(x))
                       for b in st.branches for x in b.stmts)
        return False

    def levels(self, scope: Scope) -> list[list[Stmt]]:
        stmts = scope.stmts
        level: list[int] = []
        defs = [set(id(v) for v in self.defined(s)) for s in stmts]
        refs = [self.refs_deep(s) for s in stmts]
        bars = [self.is_barrier(s) for s in stmts]
        for j in range(len(stmts)):
            lv = 0
            for i in range(j):
                if bars[i] or bars[j] or (refs[j] & defs[i]):
                    lv = max(lv, level[i] + 1)
            level.append(lv)
        out: list[list[Stmt]] = [[] for _ in range(max(level) + 1)] if level else []
        for s, lv in zip(stmts, level):
            out[lv].append(s)
        return out

    # values in JSONata
    def fact(self, e: Expr, ctx: Ctx) -> str:
        if isinstance(e, Lit):
            return jlit(e.value)
        if isinstance(e, Get):
            return self.fact_get(ctx.loc(e.src), e.path)
        if isinstance(e, Length):
            return self.fact_op(e.arg, "$len", ctx)
        if isinstance(e, Struct):
            return self.val(e, ctx)
        return self.cond(e, ctx)

    def fact_op(self, e: Expr, op: str, ctx: Ctx) -> str:
        """``$null`` / ``$bool`` / ``$len`` of a value, computed where the value lives."""
        if isinstance(e, Lit):
            v = e.value
            return jlit({"$null": v is None, "$bool": bool(v), "$len": len(v) if hasattr(v, "__len__") else 0}[op])
        if isinstance(e, Get):
            loc = ctx.loc(e.src)
            if loc.kind == "mapout" and not e.path and op in ("$len", "$bool"):
                n = f"$count({loc.expr}.v)"
                return n if op == "$len" else f"({n} > 0)"
            if loc.kind == "raw":
                x = loc.expr + jpath(e.path)
                return {
                    "$null": f"($not($exists({x})) or {x} = null)",
                    "$bool": f"$boolean({x})",
                    "$len": f"$count({x})",
                }[op]
            return self.fact_get(loc, e.path + (op,))
        if op == "$bool":
            return f"$boolean({self.fact(e, ctx)})"
        raise CompileError(f"can't evaluate {op[1:]} of {e!r} in a condition")

    def fact_get(self, loc: Loc, path: tuple) -> str:
        if loc.kind == "raw":
            if path and path[-1] in ("$null", "$bool", "$len"):
                raise CompileError("internal: op fact on raw value")
            return loc.expr + jpath(path)
        if loc.kind in ("wrapper", "carrier"):
            assert loc.reg is not None
            return f"{loc.expr}.f.{loc.reg.key(path)}"
        if loc.kind == "params":
            if not path or not isinstance(path[0], str):
                raise CompileError("a condition must read a named workflow parameter")
            reg = self.input_regs.setdefault((id(loc.node), path[0]), FactReg())
            return f"{loc.expr}.{path[0]}.f.{reg.key(path[1:])}"
        if loc.kind == "outcome":
            head = path[0] if path else None
            if head == "ok":
                return f"{loc.expr}.ok"
            if head == "error":
                return f"{loc.expr}.error"
            if head == "v":
                assert loc.vloc is not None
                return self.fact_get(loc.vloc.at(f"{loc.expr}.v"), path[1:])
            raise CompileError("conditions can read an attempt's error name but not its cause; pass the cause to a step")
        if loc.kind == "mapout":
            raise CompileError("a condition on a map's results can only use length(...); add a step to inspect them")
        if loc.kind == "dmapout":
            raise CompileError("Distributed Map results can't be read by a condition; pass them to a step")
        raise CompileError(f"internal: no facts for {loc.kind}")

    def val(self, e: Expr, ctx: Ctx) -> str:
        """JSONata producing the content to hand to a step, a carrier or the output."""
        if isinstance(e, Lit):
            return jlit(e.value)
        if isinstance(e, Get):
            loc = ctx.loc(e.src)
            path = e.path
            if not path:
                return loc.expr
            if loc.kind in ("raw", "params") and isinstance(e.src, InputV) and isinstance(path[0], str):
                # workflow params are always present (defaults are merged in at Init)
                head = loc.expr + jpath(path[:1])
                return head if len(path) == 1 else self.pj(head, path[1:])
            if loc.kind == "outcome":
                head, rest = path[0], path[1:]
                base = f"{loc.expr}.{head}"
                if head in ("ok", "error") and not rest:
                    return base
                return self.pj(base, rest) if rest else base
            return self.pj(loc.expr, path)
        if isinstance(e, Struct):
            def build(v):
                if isinstance(v, Expr):
                    return self.val(v, ctx)
                if isinstance(v, dict):
                    return "{" + ", ".join(f"{jlit(k)}: {build(x)}" for k, x in v.items()) + "}"
                return "[" + ", ".join(build(x) for x in v) + "]"

            return build(e.value)
        if isinstance(e, Length):
            return self.fact(e, ctx)
        return self.cond(e, ctx)

    @staticmethod
    def pj(base: str, path: tuple) -> str:
        return f'{{"$sw": 1, "pj": {base}, "p": {jlit(list(path))}}}'

    def cond(self, c: Expr, ctx: Ctx) -> str:
        if isinstance(c, Cmp):
            op = {"==": "=", "!=": "!="}.get(c.op, c.op)
            return f"({self.fact(c.a, ctx)} {op} {self.fact(c.b, ctx)})"
        if isinstance(c, And):
            return f"({self.cond(c.a, ctx)} and {self.cond(c.b, ctx)})"
        if isinstance(c, Or):
            return f"({self.cond(c.a, ctx)} or {self.cond(c.b, ctx)})"
        if isinstance(c, Not):
            return f"$not({self.cond(c.a, ctx)})"
        if isinstance(c, IsNull):
            return f"({self.fact_op(c.a, '$null', ctx)} = true)"
        if isinstance(c, Truthy):
            return f"({self.fact_op(c.a, '$bool', ctx)} = true)"
        if isinstance(c, Contains):
            return f"({self.fact(c.item, ctx)} in {self.fact(c.container, ctx)})"
        if isinstance(c, OneOf):
            return f"({self.fact(c.a, ctx)} in {jlit(list(c.values))})"
        if isinstance(c, IsOk):
            return f"({self.fact_get(ctx.loc(c.o.src), ('ok',))} = true)"
        if isinstance(c, Get):
            return f"({self.fact_op(c, '$bool', ctx)} = true)"
        raise CompileError(f"can't express {c!r} as a condition")

    def carrier(self, e: Expr | None, ctx: Ctx, reg: FactReg, extra: dict[str, str] | None = None,
                force: bool = False) -> str:
        """Copy a value into a container; add facts conditions registered on the container."""
        v = "null" if e is None else self.val(e, ctx)
        if not reg and not extra and not force:
            return v
        parts = {"$sw": "1", "x": v}
        if reg:
            parts["f"] = jobj({k: self.fact_path(e, list(p), ctx) for p, k in reg.paths.items()})
        parts.update(extra or {})
        return jobj(parts)

    def fact_path(self, e: Expr | None, path: list, ctx: Ctx) -> str:
        if e is None:
            return "null"
        if path and path[-1] in ("$null", "$bool", "$len"):
            op = path[-1]
            inner: Expr = Get(e.src, e.path + tuple(path[:-1])) if isinstance(e, Get) else e
            if isinstance(e, Lit):
                from sfnkit.local import index

                inner = Lit(index(e.value, tuple(path[:-1])))
            return self.fact_op(inner, op, ctx)
        if isinstance(e, Get):
            return self.fact(Get(e.src, e.path + tuple(path)), ctx)
        if isinstance(e, Lit):
            from sfnkit.local import index

            return jlit(index(e.value, tuple(path)))
        if not path:
            return self.fact(e, ctx)
        raise CompileError(f"can't read {path} from {e!r} in a condition")

    # machines
    def compile_machine(self, m: Machine, graph: Graph, mode: str) -> None:
        """mode "root": the execution input is raw JSON. mode "call": a reusable child started by a
        parent; each parameter arrives as a carrier so conditions in the child can read facts."""
        self.analyse(graph.root)
        self.machine = m
        ctx = Ctx("")
        defaults = jlit(graph.input.defaults)
        init_assign: dict[str, Any] = {"input": J(f"$merge([{defaults}, $states.input])")}
        if mode == "root":
            ctx.syms[id(graph.input)] = Loc("$input", "raw")
        else:
            ctx.syms[id(graph.input)] = Loc("$input", "params", node=graph.workflow)
        init = Frag.single(self.state_name("Init", m.name), {"Type": "Pass", "Assign": init_assign})
        body = self.scope(graph.root, ctx)
        result = graph.root.result
        if mode == "root":
            out = J(lambda: self.val(result, ctx) if result is not None else "null")
        else:
            reg = self.result_regs.setdefault(id(graph.workflow), FactReg())
            out = J(lambda: self.carrier(result, ctx, reg))
        start, states = close(seq([init, body]), self.state_name("Done", m.name), {"Type": "Pass", "Output": out})
        m.definition = {"Comment": f"sfnkit {m.kind} {m.name}", "QueryLanguage": "JSONata",
                        "StartAt": start, "States": states}

    def scope(self, scope: Scope, ctx: Ctx) -> Frag | None:
        frags: list[Frag | None] = []
        for level in self.levels(scope):
            if len(level) == 1:
                frags.append(self.stmt(level[0], ctx))
            else:
                frags.append(self.parallel(level, ctx))
        return seq(frags)

    def parallel(self, level: list[Stmt], ctx: Ctx) -> Frag:
        self.pids += 1
        pid = self.pids
        branches = []
        assign: dict[str, Any] = {}
        lifted: list[tuple[Any, str, Ctx]] = []
        for k, st in enumerate(level):
            bctx = Ctx(f"{ctx.prefix}p{pid}b{k}_", ctx)
            frag = self.stmt(st, bctx)
            exported = [v for v in self.defined(st) if self.used_outside(v, st)]
            out = J(lambda ex=exported, bc=bctx: jobj({v.id: bc.loc(v).expr for v in ex}))
            start, states = close(frag, self.state_name("BranchDone", f"p{pid}b{k}"), {"Type": "Pass", "Output": out})
            branches.append({"StartAt": start, "States": states})
            for v in exported:
                outer = ctx.var(v.id)
                assign[outer] = J(f"$states.result[{k}].{v.id}")
                lifted.append((v, outer, bctx))
        for v, outer, bctx in lifted:
            ctx.syms[id(v)] = bctx.loc(v).at(f"${outer}")
        name = self.state_name("Parallel", f"p{pid}")
        return Frag.single(name, {"Type": "Parallel", "Branches": branches, "Assign": assign, "Output": J("{}")})

    def stmt(self, st: Stmt, ctx: Ctx) -> Frag | None:
        if isinstance(st, TaskN):
            return self.task(st, ctx)
        if isinstance(st, MapN):
            return self.map(st, ctx)
        if isinstance(st, CallN):
            return self.call(st, ctx)
        if isinstance(st, SubN):
            return self.sub(st, ctx)
        if isinstance(st, AttemptN):
            return self.attempt(st, ctx)
        if isinstance(st, ChoiceN):
            return self.choice(st, ctx)
        if isinstance(st, WaitN):
            return self.wait(st, ctx)
        if isinstance(st, FailN):
            return self.fail(st, ctx)
        raise CompileError(f"internal: unknown statement {st!r}")

    def envelope(self, st: TaskN, ctx: Ctx) -> J:
        args = {k: self.val(e, ctx) for k, e in st.args.items()}
        treg, ireg = self.treg(st), self.ireg(st)

        def render():
            fields = {
                "sw": "1",
                "step": jlit(st.step.key),
                "execution": "$states.context.Execution.Name",
                "node": jlit(st.id),
                "args": jobj(args),
            }
            if treg:
                fields["facts"] = jlit(treg.spec())
            if ireg.facets:
                fields["items"] = jlit(ireg.spec())
            return jobj(fields)

        return J(render)

    def task_state(self, st: TaskN, ctx: Ctx) -> tuple[dict, str]:
        fn = self.function_for(st)
        backend = backend_for(fn.compute)
        ts = backend.task_state(fn)
        payload = self.envelope(st, ctx)
        args = _replace(ts.arguments, payload)
        state: dict[str, Any] = {"Type": "Task", "Resource": ts.resource, "Arguments": args}
        if ts.timeout_seconds:
            state["TimeoutSeconds"] = ts.timeout_seconds
        retry = [r.to_asl() for r in st.step.retry] + backend.default_retry()
        if retry:
            state["Retry"] = retry
        self.machine.substitutions.update(ts.substitutions)
        if fn not in self.machine.functions:
            self.machine.functions.append(fn)
        return state, ts.result

    def task(self, st: TaskN, ctx: Ctx) -> Frag:
        state, result = self.task_state(st, ctx)
        var = ctx.var(st.id)
        state["Assign"] = {var: J(result)}
        state["Output"] = J("{}")
        ctx.syms[id(st)] = Loc(f"${var}", "wrapper", self.treg(st), st)
        return Frag.single(self.state_name(st.step.name, st.id), state)

    def items_source(self, m: MapN, ctx: Ctx) -> tuple[dict, Loc]:
        from sfnkit.blocks import S3File, S3Objects

        src = m.items
        dist = m.distributed
        if isinstance(src, S3Objects):
            self.machine.s3_reads.append((src.bucket, src.prefix))
            return {"ItemReader": {"Resource": "arn:aws:states:::s3:listObjectsV2",
                                   "Arguments": {"Bucket": src.bucket, "Prefix": src.prefix}}}, Loc("", "raw")
        if isinstance(src, S3File):
            self.machine.s3_reads.append((src.bucket, src.key))
            cfg: dict[str, Any] = {"InputType": {"json": "JSON", "jsonl": "JSONL", "csv": "CSV"}[src.format]}
            if src.format == "csv":
                cfg["CSVHeaderLocation"] = "FIRST_ROW"
            return {"ItemReader": {"Resource": "arn:aws:states:::s3:getObject", "ReaderConfig": cfg,
                                   "Arguments": {"Bucket": src.bucket, "Key": src.key}}}, Loc("", "raw")
        if isinstance(src, Lit):
            if not isinstance(src.value, list):
                raise CompileError("each(...) items must be a list")
            return {"Items": J(jlit(src.value))}, Loc("", "raw")
        if isinstance(src, Struct):
            return {"Items": J(self.val(src, ctx))}, Loc("", "raw")
        if isinstance(src, Get):
            loc = ctx.loc(src.src)
            if loc.kind == "raw":
                return {"Items": J(loc.expr + jpath(src.path))}, Loc("", "raw")
            if loc.kind == "wrapper":
                key, ereg = self.ireg(loc.node).facet(src.path, "s3" if dist else "inline")
                eloc = Loc("", "wrapper", ereg)
                if dist:
                    reader = {"Resource": "arn:aws:states:::s3:getObject", "ReaderConfig": {"InputType": "JSON"},
                              "Arguments": {"Bucket": J(f"{loc.expr}.it.{key}.bucket"),
                                            "Key": J(f"{loc.expr}.it.{key}.key")}}
                    self.machine.substitutions["ArtifactsBucket"] = {"Ref": "ArtifactsBucket"}
                    return {"ItemReader": reader}, eloc
                return {"Items": J(f"{loc.expr}.it.{key}")}, eloc
            if loc.kind == "mapout" and not src.path:
                ereg = self.elem_regs.setdefault(id(loc.node), FactReg())
                return {"Items": J(f"{loc.expr}.v")}, Loc("", "carrier", ereg)
        raise CompileError(
            "each(...)/map items must be a workflow input, a literal list, a step's output (or a field of it), "
            "or another inline map's results"
        )

    def map(self, m: MapN, ctx: Ctx) -> Frag:
        fields, item_loc = self.items_source(m, ctx)
        prefix = f"{ctx.prefix}{m.id}_"
        bctx = Ctx(prefix)
        free = self.free_values(m.body, {})
        selector: dict[str, Any] = {
            "item": J("$states.context.Map.Item.Value"),
            "index": J("$states.context.Map.Item.Index"),
        }
        if free:
            selector["free"] = J(lambda fr=free: jobj({v.id: ctx.loc(v).expr for v in fr}))
        assign: dict[str, Any] = {bctx.var("item"): J("$states.input.item"), bctx.var("index"): J("$states.input.index")}
        for v in free:
            var = bctx.var("v_" + v.id)
            assign[var] = J(f"$states.input.free.{v.id}")
            bctx.syms[id(v)] = ctx.loc(v).at(f"${var}")
        bctx.syms[id(m.item)] = item_loc.at(f"${bctx.var('item')}")
        init = Frag.single(self.state_name("Item", m.id), {"Type": "Pass", "Assign": assign})
        body = self.scope(m.body, bctx)
        ereg = self.elem_regs.setdefault(id(m), FactReg())
        extra = {"i": f"${bctx.var('index')}"} if m.distributed else None
        out = J(lambda: self.carrier(m.body.result, bctx, ereg, extra))
        start, states = close(seq([init, body]), self.state_name("ItemDone", m.id), {"Type": "Pass", "Output": out})
        proc: dict[str, Any] = {"StartAt": start, "States": states}
        state: dict[str, Any] = {"Type": "Map", **fields, "ItemSelector": selector}
        var = ctx.var(m.id)
        if m.distributed:
            proc["ProcessorConfig"] = {"Mode": "DISTRIBUTED", "ExecutionType": m.opts.get("child_type") or "STANDARD"}
            state["ResultWriter"] = {
                "Resource": "arn:aws:states:::s3:putObject",
                "Arguments": {"Bucket": "${ArtifactsBucket}",
                              "Prefix": J(f"'runs/' & $states.context.Execution.Name & '/{m.id}/results'")},
            }
            if m.opts.get("tolerated_failure_percentage") is not None:
                state["ToleratedFailurePercentage"] = m.opts["tolerated_failure_percentage"]
            if m.opts.get("tolerated_failure_count") is not None:
                state["ToleratedFailureCount"] = m.opts["tolerated_failure_count"]
            state["Assign"] = {var: J('{"$sw": 1, "dmap": $states.result.ResultWriterDetails}')}
            self.machine.map_run = True
            self.machine.substitutions["ArtifactsBucket"] = {"Ref": "ArtifactsBucket"}
            ctx.syms[id(m)] = Loc(f"${var}", "dmapout", node=m)
        else:
            proc["ProcessorConfig"] = {"Mode": "INLINE"}
            state["Assign"] = {var: J('{"$sw": 1, "v": $states.result}')}
            ctx.syms[id(m)] = Loc(f"${var}", "mapout", node=m)
        state["ItemProcessor"] = proc
        if m.opts.get("max_concurrency"):
            state["MaxConcurrency"] = m.opts["max_concurrency"]
        state["Output"] = J("{}")
        return Frag.single(self.state_name("Map", m.id), state)

    def start_child(self, child_name: str, input_j: J) -> dict:
        ph = f"Sm{_camel(child_name)}Arn"
        from sfnkit.app import sm_arn

        self.machine.substitutions[ph] = sm_arn(child_name)
        if child_name not in self.machine.children:
            self.machine.children.append(child_name)
        return {"Type": "Task", "Resource": "arn:aws:states:::states:startExecution.sync:2",
                "Arguments": {"StateMachineArn": f"${{{ph}}}", "Input": input_j}}

    def call(self, c: CallN, ctx: Ctx) -> Frag:
        child_name, child_wf = self.child_machine_for(c.workflow)
        regs = {p: self.input_regs.setdefault((id(child_wf), p), FactReg()) for p in c.args}

        def render():
            fields = {p: self.carrier(e, ctx, regs[p], force=True) for p, e in c.args.items()}
            fields["AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID"] = "$states.context.Execution.Id"
            return jobj(fields)

        state = self.start_child(child_name, J(render))
        var = ctx.var(c.id)
        state["Assign"] = {var: J("$states.result.Output")}
        state["Output"] = J("{}")
        reg = self.result_regs.setdefault(id(child_wf), FactReg())
        ctx.syms[id(c)] = Loc(f"${var}", "carrier", reg)
        return Frag.single(self.state_name(c.workflow.name, c.id), state)

    def sub(self, s: SubN, ctx: Ctx) -> Frag:
        free = self.free_values(s.body, {})
        exports = [v for v in self.sub_defined(s.body) if self.used_outside(v, s)]
        name = self.sub_name_for(s)
        child = Machine(name=name, kind="sub", type=s.type)
        outer_machine = self.machine
        outer_pids = self.pids
        # compile the child with its own variable context
        self.machines.append(child)
        self.machine = child
        cctx = Ctx("")
        init_assign: dict[str, Any] = {}
        for v in free:
            var = cctx.var("v_" + v.id)
            init_assign[var] = J(f"$states.input.free.{v.id}")
            cctx.syms[id(v)] = ctx.loc(v).at(f"${var}")
        init = Frag.single(self.state_name("Init", name), {"Type": "Pass", "Assign": init_assign})
        body = self.scope(s.body, cctx)
        out = J(lambda: jobj({v.id: cctx.loc(v).expr for v in exports}))
        start, states = close(seq([init, body]), self.state_name("Done", name), {"Type": "Pass", "Output": out})
        child.definition = {"Comment": f"sfnkit subworkflow {s.name}", "QueryLanguage": "JSONata",
                            "StartAt": start, "States": states}
        self.machine = outer_machine
        self.pids = max(self.pids, outer_pids)

        def render():
            return jobj({"free": jobj({v.id: ctx.loc(v).expr for v in free}),
                         "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID": "$states.context.Execution.Id"})

        state = self.start_child(name, J(render))
        var = ctx.var(s.id)
        state["Assign"] = {var: J("$states.result.Output")}
        state["Output"] = J("{}")
        for v in exports:
            ctx.syms[id(v)] = cctx.loc(v).at(f"${var}.{v.id}")
        return Frag.single(self.state_name(s.name, s.id), state)

    def attempt(self, a: AttemptN, ctx: Ctx) -> Frag:
        ovar = ctx.var(a.id)
        errors = a.handled if a.handled is not None else ["States.ALL"]
        if not errors:
            errors = ["States.ALL"]
        catch_assign = {ovar: J('{"ok": false, "v": null, "error": $states.errorOutput.Error, '
                                '"cause": {"$sw": 1, "c": $states.errorOutput.Cause}}')}
        catch = {"ErrorEquals": errors, "Assign": catch_assign, "Output": J("{}")}
        body = a.body
        single = (
            len(body.stmts) == 1
            and isinstance(body.stmts[0], TaskN)
            and isinstance(body.result, Get)
            and body.result.src is body.stmts[0]
            and not body.result.path
        )
        if single:
            t: TaskN = body.stmts[0]  # type: ignore[assignment]
            state, result = self.task_state(t, ctx)
            state["Assign"] = {ovar: J(f'{{"ok": true, "v": {result}, "error": null, "cause": null}}')}
            vloc = Loc("", "wrapper", self.treg(t), t)
        else:
            self.pids += 1
            bctx = Ctx(f"{ctx.prefix}{a.id}_", ctx)
            frag = self.scope(body, bctx)
            oreg = self.ok_regs.setdefault(id(a), FactReg())
            out = J(lambda: self.carrier(body.result, bctx, oreg))
            start, states = close(frag, self.state_name("AttemptDone", a.id), {"Type": "Pass", "Output": out})
            state = {"Type": "Parallel", "Branches": [{"StartAt": start, "States": states}]}
            state["Assign"] = {ovar: J('{"ok": true, "v": $states.result[0], "error": null, "cause": null}')}
            vloc = Loc("", "carrier", oreg)
        state["Output"] = J("{}")
        state["Catch"] = [catch]
        ctx.syms[id(a)] = Loc(f"${ovar}", "outcome", vloc=vloc)
        name = self.state_name("Attempt", a.id)

        def set_next(nxt, s=state, c=catch):
            s["Next"] = nxt
            c["Next"] = nxt

        return Frag({name: state}, name, [set_next])

    def choice(self, c: ChoiceN, ctx: Ctx) -> Frag:
        name = self.state_name("Choice", c.id)
        choice: dict[str, Any] = {"Type": "Choice", "Choices": []}
        states: dict[str, dict] = {name: choice}
        exits: list[Callable[[str], None]] = []
        entries: dict[int, Any] = {}
        for b, scope in enumerate(c.branches):
            frag = self.scope(scope, ctx)
            if c.phis:
                regs = [self.phi_regs.setdefault(id(p), FactReg()) for p in c.phis]
                assign = {
                    ctx.var(p.id): J(lambda p=p, r=r, b=b: self.carrier(p.values[b], ctx, r))
                    for p, r in zip(c.phis, regs)
                }
                set_phis = Frag.single(self.state_name("Join", f"{c.id}b{b}"), {"Type": "Pass", "Assign": assign})
                if frag is None or frag.exits:
                    frag = seq([frag, set_phis])
            if frag is None:
                entries[b] = None
            else:
                states.update(frag.states)
                entries[b] = frag.entry
                exits.extend(frag.exits)
        for cond, b in c.rules:
            rule: dict[str, Any] = {"Condition": J(self.cond(cond, ctx))}
            choice["Choices"].append(rule)
            if entries[b] is None:
                exits.append(lambda nxt, r=rule: r.__setitem__("Next", nxt))
            else:
                rule["Next"] = entries[b]
        if entries[c.default] is None:
            exits.append(lambda nxt: choice.__setitem__("Default", nxt))
        else:
            choice["Default"] = entries[c.default]
        for p in c.phis:
            ctx.syms[id(p)] = Loc(f"${ctx.var(p.id)}", "carrier", self.phi_regs[id(p)])
        # values defined inside branches stay reachable (when the other branches end in fail)
        return Frag(states, name, exits)

    def wait(self, w: WaitN, ctx: Ctx) -> Frag:
        state: dict[str, Any] = {"Type": "Wait"}
        if w.seconds is not None:
            state["Seconds"] = w.seconds.value if isinstance(w.seconds, Lit) else J(self.fact(w.seconds, ctx))
        else:
            assert w.until is not None
            state["Timestamp"] = w.until.value if isinstance(w.until, Lit) else J(self.fact(w.until, ctx))
        return Frag.single(self.state_name("Wait", w.id), state)

    def fail(self, f: FailN, ctx: Ctx) -> Frag:
        def text(e):
            if isinstance(e, Lit):
                return e.value if isinstance(e.value, str) else json.dumps(e.value)
            return J(f"$string({self.fact(e, ctx)})")

        name = self.state_name("Fail", f.id)
        state = {"Type": "Fail", "Error": text(f.error), "Cause": text(f.cause)}
        return Frag({name: state}, name, [])


def _camel(s: str) -> str:
    return "".join(p[:1].upper() + p[1:] for p in re.split(r"[^A-Za-z0-9]+", s) if p)


def _gets(e: Expr) -> list[Get]:
    out: list[Get] = []

    def f(g):
        out.append(g)
        return g

    e.remap(f)
    return out


def _replace(obj, payload):
    if obj is PAYLOAD:
        return payload
    if isinstance(obj, dict):
        return {k: _replace(v, payload) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace(v, payload) for v in obj]
    return obj


def render(obj):
    """Replace every ``J`` with its final ``{% ... %}`` string."""
    if isinstance(obj, J):
        return obj.render()
    if isinstance(obj, dict):
        return {k: render(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [render(v) for v in obj]
    return obj


def render_fixpoint(defs: list[dict]) -> list[dict]:
    """Render until rendering stops registering new facts (carriers can register on inner values)."""
    for _ in range(20):
        before = FactReg.changes
        out = [render(d) for d in defs]
        if FactReg.changes == before:
            return out
    raise CompileError("internal: fact registration didn't settle")
