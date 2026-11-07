"""Merge traced paths into one graph.

Paths share events until a decision whose value differs between them. There the merger opens a
``ChoiceN``, finds the join (the first event every surviving path reaches again), and builds each
branch from its paths until the join. Events are matched across paths by identity (call-site stack).

A value that differs between paths at a use site becomes a phi: the paths are grouped by the first
choice (in scopes enclosing the use) where they went different ways, and each branch of that choice
assigns its version. Expressions are anti-unified first, so phis sit at the differing leaves.
"""

from __future__ import annotations

from collections import defaultdict

from sfnkit.errors import TraceError
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
    as_cond,
)
from sfnkit.iam import EMPTY, Policy
from sfnkit.ir import (
    AttemptN,
    CallN,
    ChoiceN,
    FailN,
    Graph,
    Ids,
    InputV,
    ItemV,
    MapN,
    PhiV,
    Scope,
    SubN,
    TaskN,
    WaitN,
)
from sfnkit.trace.tracer import END, DecEv, EndEv, EnterEv, ExitEv, FailEv, NodeEv, Tracer


class _Path:
    def __init__(self, t: Tracer):
        self.t = t
        self.events = t.events
        self.pos = 0
        self.env: dict[int, object] = {}
        self.history: list[tuple[ChoiceN, int]] = []
        self.ends_fail = bool(self.events) and isinstance(self.events[-1], FailEv)

    @property
    def done(self) -> bool:
        return self.pos >= len(self.events)

    def peek(self):
        return self.events[self.pos]


def merge(workflow, tracers: list[Tracer]) -> Graph:
    return _Merger(workflow, tracers).run()


class _Merger:
    def __init__(self, workflow, tracers: list[Tracer]):
        self.workflow = workflow
        self.ids = Ids()
        self.input = InputV("input", list(workflow.signature.parameters), dict(workflow.defaults))
        self.paths = [_Path(t) for t in tracers]
        for p in self.paths:
            p.env[id(p.t.input)] = self.input
        self.phi_cache: dict[tuple, PhiV] = {}

    def run(self) -> Graph:
        root = Scope("root", None)
        root.policy = self.workflow.iam
        self.build(self.paths, END, root)
        live = [p for p in self.paths if not p.done]
        if live:
            root.result = self.resolve([(p, p.peek().result) for p in live], root)
        else:
            root.result = Lit(None)
        _simplify(root)
        return Graph(self.workflow, root, self.input)

    # structure
    def build(self, paths: list[_Path], stop, scope: Scope) -> None:
        while True:
            live = [p for p in paths if not p.done]
            if not live:
                return
            idents = [p.peek().identity for p in live]
            if stop is not None and all(i == stop for i in idents):
                return
            first = idents[0]
            if any(i != first for i in idents):
                raise TraceError(self._diverged(live))
            ev = live[0].peek()
            if isinstance(ev, NodeEv):
                self.node(live, scope)
            elif isinstance(ev, DecEv):
                if len({p.peek().value for p in live}) == 1:
                    for p in live:
                        p.pos += 1
                    continue
                self.choice(live, scope)
            elif isinstance(ev, EnterEv):
                self.enter(live, scope)
            elif isinstance(ev, FailEv):
                err = self.resolve([(p, p.peek().error) for p in live], scope)
                cause = self.resolve([(p, p.peek().cause) for p in live], scope)
                scope.stmts.append(FailN(self.ids.next("f"), scope, err, cause))
                for p in live:
                    p.pos = len(p.events)
            elif isinstance(ev, (ExitEv, EndEv)):
                raise TraceError("internal: block ended where it wasn't expected")
            else:
                raise TraceError(f"internal: unknown event {ev!r}")

    def _diverged(self, live: list[_Path]) -> str:
        lines = []
        for p in live[:3]:
            ev = p.peek()
            what = getattr(getattr(ev, "tnode", None), "label", type(ev).__name__)
            where = getattr(getattr(ev, "tnode", None), "where", "") or getattr(ev, "where", "")
            lines.append(f"  {what} {where}")
        return (
            "the workflow ran different steps on different paths without a condition deciding between "
            "them. This usually means a Python loop whose length depends on a condition, or "
            "non-deterministic code (randomness, time, I/O) in the workflow body:\n" + "\n".join(lines)
        )

    def join(self, live: list[_Path]):
        nonfail = [p for p in live if not p.ends_fail]
        if not nonfail:
            return None
        ref = nonfail[0]
        ref_ids = [e.identity for e in ref.events[ref.pos :]]
        sets = [set(e.identity for e in p.events[p.pos :]) for p in live]
        for i, ident in enumerate(ref_ids):
            later = None
            ok = True
            for p, s in zip(live, sets):
                if ident in s:
                    continue
                if p.ends_fail:
                    # A failing path is excused only if it never rejoins the reference path later.
                    if later is None:
                        later = set(ref_ids[i:])
                    if s.isdisjoint(later):
                        continue
                ok = False
                break
            if ok:
                return ident
        return None

    def choice(self, live: list[_Path], scope: Scope) -> None:
        ev0 = live[0].peek()
        cond = as_cond(self.resolve([(p, p.peek().cond) for p in live], scope))
        T = [p for p in live if p.peek().value]
        F = [p for p in live if not p.peek().value]
        for p in live:
            p.pos += 1
        stop = self.join(live)
        c = ChoiceN(self.ids.next("c"), scope, ev0.where)
        scope.stmts.append(c)
        bt, bf = Scope("branch", c), Scope("branch", c)
        c.rules = [(cond, 0)]
        c.branches = [bt, bf]
        c.default = 1
        for p in T:
            p.history.append((c, 0))
        for p in F:
            p.history.append((c, 1))
        self.build(T, stop, bt)
        self.build(F, stop, bf)

    def policy_for(self, step, scope: Scope) -> Policy:
        if step.iam is not None:
            return step.iam
        for s in scope.ancestors():
            if s.policy is not None:
                return s.policy
        return EMPTY

    def node(self, live: list[_Path], scope: Scope) -> None:
        tns = [p.peek().tnode for p in live]
        t0 = tns[0]
        nid = self.ids.next("n")

        def args():
            return {
                k: self.resolve([(p, tn.args[k]) for p, tn in zip(live, tns)], scope) for k in t0.args
            }

        if t0.kind == "task":
            n = TaskN(nid, scope, t0.step, args(), self.policy_for(t0.step, scope), t0.where)
        elif t0.kind == "call":
            n = CallN(nid, scope, t0.workflow, args(), t0.where)
        elif t0.kind == "wait":
            sec = t0.opts["seconds"]
            until = t0.opts["until"]
            n = WaitN(
                self.ids.next("w"),
                scope,
                None if sec is None else self.resolve([(p, tn.opts["seconds"]) for p, tn in zip(live, tns)], scope),
                None if until is None else self.resolve([(p, tn.opts["until"]) for p, tn in zip(live, tns)], scope),
                t0.where,
            )
        else:
            raise TraceError(f"internal: unexpected node kind {t0.kind}")
        scope.stmts.append(n)
        for p, tn in zip(live, tns):
            p.env[id(tn)] = n
            p.pos += 1

    def enter(self, live: list[_Path], scope: Scope) -> None:
        tns = [p.peek().tnode for p in live]
        t0 = tns[0]
        if t0.kind == "each":
            items = t0.items
            if isinstance(items, Expr):
                items = self.resolve([(p, tn.items) for p, tn in zip(live, tns)], scope)
            n = MapN(self.ids.next("m"), scope, items, dict(t0.opts), t0.where)
            n.body = Scope("each", n)
            n.item = ItemV(self.ids.next("item"), n)
            for p, tn in zip(live, tns):
                p.env[id(tn)] = n
                p.env[id(tn.item)] = n.item
        elif t0.kind == "attempt":
            n = AttemptN(self.ids.next("a"), scope, t0.handled, t0.where)
            n.body = Scope("attempt", n)
            for p, tn in zip(live, tns):
                p.env[id(tn)] = n
        elif t0.kind == "sub":
            n = SubN(self.ids.next("s"), scope, t0.opts["name"], t0.opts["iam"], t0.opts["type"], t0.where)
            n.body = Scope("sub", n)
            n.body.policy = t0.opts["iam"]
            for p, tn in zip(live, tns):
                p.env[id(tn)] = n
        else:
            raise TraceError(f"internal: unexpected block kind {t0.kind}")
        scope.stmts.append(n)
        for p in live:
            p.pos += 1
        exit_ident = t0.identity + ("exit",)
        self.build(live, exit_ident, n.body)
        at_exit = [p for p in live if not p.done]
        if at_exit and t0.kind != "sub":
            n.body.result = self.resolve(
                [(p, p.peek().result if p.peek().result is not None else Lit(None)) for p in at_exit], n.body
            )
        for p in at_exit:
            p.pos += 1

    # values
    def lookup(self, p: _Path, src):
        try:
            return p.env[id(src)]
        except KeyError:
            raise TraceError(f"internal: value {getattr(src, 'label', src)!r} has no graph node") from None

    def resolve(self, pairs, ctx: Scope) -> Expr:
        mapped = [(p, e.remap(lambda g, p=p: Get(self.lookup(p, g.src), g.path))) for p, e in pairs]
        return self.unify(mapped, ctx)

    def unify(self, pairs, ctx: Scope) -> Expr:
        e0 = pairs[0][1]
        k0 = e0.key()
        if all(e.key() == k0 for _, e in pairs):
            return e0
        cls = type(e0)
        if all(type(e) is cls for _, e in pairs):
            sub = lambda attr: self.unify([(p, getattr(e, attr)) for p, e in pairs], ctx)
            if cls is Cmp and all(e.op == e0.op for _, e in pairs):
                return Cmp(e0.op, sub("a"), sub("b"))
            if cls in (And, Or):
                return cls(as_cond(sub("a")), as_cond(sub("b")))
            if cls is Not:
                return Not(as_cond(sub("a")))
            if cls in (IsNull, Truthy):
                return cls(sub("a"))
            if cls is Contains:
                return Contains(sub("container"), sub("item"))
            if cls is OneOf and all(e.values == e0.values for _, e in pairs):
                return OneOf(sub("a"), e0.values)
            if cls is IsOk:
                return IsOk(sub("o"))  # type: ignore[arg-type]
            if cls is Length:
                return Length(sub("arg"))
            if cls is Struct and all(_shape(e.value) == _shape(e0.value) for _, e in pairs):
                return _unify_struct(self, pairs, ctx)
        return self.phi(pairs, ctx)

    def phi(self, pairs, ctx: Scope) -> Expr:
        # Only choices in scopes enclosing the use site matter; every path reaching it went through
        # those in the same order, so the filtered histories line up.
        chain = {id(s) for s in ctx.ancestors()}
        hs = [[(c, b) for (c, b) in p.history if id(c.scope) in chain] for p, _ in pairs]
        n = len(hs[0])
        if any(len(h) != n or any(h[i][0] is not hs[0][i][0] for i in range(n)) for h in hs):
            raise TraceError("internal: paths reaching the same point went through different conditions")
        # The latest choice where the paths split is where differing definitions meet. Earlier
        # splits are resolved recursively inside each branch (their phis are assigned before it).
        split = [i for i in range(n) if len({h[i][1] for h in hs}) > 1]
        if not split:
            raise TraceError(
                "a value is assigned differently on different paths, but no condition separates those "
                "paths at this point"
            )
        idx = split[-1]
        c = hs[0][idx][0]
        groups: dict[int, list] = defaultdict(list)
        for pair, h in zip(pairs, hs):
            groups[h[idx][1]].append(pair)
        values = {b: self.unify(grp, c.branches[b]) for b, grp in groups.items()}
        first = next(iter(values.values()))
        if len(values) == len(c.branches) and all(v.key() == first.key() for v in values.values()):
            return first
        for b in range(len(c.branches)):
            values.setdefault(b, Lit(None))
        cache_key = (id(c),) + tuple((b, values[b].key()) for b in sorted(values))
        phi = self.phi_cache.get(cache_key)
        if phi is None:
            phi = PhiV(self.ids.next("phi"), c)
            phi.values = values
            c.phis.append(phi)
            self.phi_cache[cache_key] = phi
        return Get(phi)


def _shape(v):
    if isinstance(v, dict):
        return ("d", tuple(sorted((k, _shape(x)) for k, x in v.items())))
    if isinstance(v, list):
        return ("l", tuple(_shape(x) for x in v))
    return "e"


def _unify_struct(m: _Merger, pairs, ctx):
    def rec(vals):
        v0 = vals[0][1]
        if isinstance(v0, dict):
            return {k: rec([(p, v[k]) for p, v in vals]) for k in v0}
        if isinstance(v0, list):
            return [rec([(p, v[i]) for p, v in vals]) for i in range(len(v0))]
        return m.unify(vals, ctx)

    out = Struct.__new__(Struct)
    out.value = rec([(p, e.value) for p, e in pairs])
    return out


# simplification


def _simplify(scope: Scope) -> None:
    _flatten(scope)
    _prune_phis(scope)


def _flatten(scope: Scope) -> None:
    for st in scope.stmts:
        for cs in st.child_scopes():
            _flatten(cs)
    for st in scope.stmts:
        if isinstance(st, ChoiceN):
            while _flatten_once(st):
                pass


def stmt_exprs(st) -> list:
    """Expressions a statement evaluates itself (not those of nested scopes, nor phi values)."""
    if isinstance(st, (TaskN, CallN)):
        return list(st.args.values())
    if isinstance(st, MapN):
        return [st.items] if isinstance(st.items, Expr) else []
    if isinstance(st, WaitN):
        return [e for e in (st.seconds, st.until) if e is not None]
    if isinstance(st, FailN):
        return [st.error, st.cause]
    if isinstance(st, ChoiceN):
        return [c for c, _ in st.rules]
    return []


def _prune_phis(root: Scope) -> None:
    """Drop phis nothing reads (flattening and OR-patterns leave some behind)."""
    used: set[int] = set()
    pending: list[PhiV] = []

    def mark(e):
        if e is None:
            return
        for g in _gets(e):
            if isinstance(g.src, PhiV) and id(g.src) not in used:
                used.add(id(g.src))
                pending.append(g.src)

    def walk(s: Scope):
        for st in s.stmts:
            for e in stmt_exprs(st):
                mark(e)
            for cs in st.child_scopes():
                walk(cs)
        mark(s.result)

    walk(root)
    while pending:
        for v in pending.pop().values.values():
            mark(v)

    def prune(s: Scope):
        for st in s.stmts:
            if isinstance(st, ChoiceN):
                st.phis = [p for p in st.phis if id(p) in used]
            for cs in st.child_scopes():
                prune(cs)

    prune(root)


def _gets(e: Expr) -> list[Get]:
    out: list[Get] = []

    def f(g):
        out.append(g)
        return g

    e.remap(f)
    return out


def _flatten_once(c: ChoiceN) -> bool:
    """``if a: A / else: (if b: B / else: C)`` -> one Choice with rules [a->A, b->B], default C."""
    d = c.default
    dscope = c.branches[d]
    if len(dscope.stmts) != 1 or not isinstance(dscope.stmts[0], ChoiceN):
        return False
    c2: ChoiceN = dscope.stmts[0]  # type: ignore[assignment]
    old = c.branches
    keep = [i for i in range(len(old)) if i != d]
    remap = {old_i: new_i for new_i, old_i in enumerate(keep)}
    base = len(keep)
    c.branches = [old[i] for i in keep] + list(c2.branches)
    for b in c2.branches:
        b.parent = c
    c.rules = [(cond, remap[i]) for cond, i in c.rules] + [(cond, base + j) for cond, j in c2.rules]
    c.default = base + c2.default
    c2_phis = {id(p): p for p in c2.phis}
    for phi in c.phis:
        vals = {remap[i]: v for i, v in phi.values.items() if i != d}
        vd = phi.values[d]
        for j in range(len(c2.branches)):
            vals[base + j] = _subst_phis(vd, c2_phis, j)
        phi.values = vals
    for p2 in c2.phis:
        p2.choice = c
        p2.values = {**{remap[i]: Lit(None) for i in keep}, **{base + j: v for j, v in p2.values.items()}}
        c.phis.append(p2)
    return True


def _subst_phis(e: Expr, phis: dict[int, PhiV], j: int) -> Expr:
    def f(g: Get):
        p = phis.get(id(g.src))
        if p is None:
            return g
        v = p.values[j]
        if not g.path:
            return v
        if isinstance(v, Get):
            return Get(v.src, v.path + g.path)
        return g  # keep the phi reference; it still holds the right value in that branch

    return e.remap(f)
