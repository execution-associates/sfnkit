"""Static checks on a workflow body, run when ``@workflow`` decorates it.

Tracing decides branches through ``==`` and ``bool()``, so syntax that bypasses them (``is``, ``case
None``, sequence/mapping patterns...) would silently take the wrong branch. These are rejected here.
The lint also reads each ``match attempt(...)`` to learn which error names it handles, so errors it
doesn't handle are not caught (they fail the workflow, as Python would leave them unhandled).
"""

from __future__ import annotations

import ast
import inspect
import warnings
from dataclasses import dataclass, field

from sfnkit.errors import LintError, TraceError


@dataclass
class LintInfo:
    # (filename, lineno, col) of an attempt(...) call -> handled error names, or None for catch-all
    attempts: dict[tuple[str, int, int], list[str] | None] = field(default_factory=dict)
    skipped: bool = False

    def attempt_handled(self, frame) -> list[str] | None:
        code = frame.f_code
        positions = list(code.co_positions())
        idx = frame.f_lasti // 2
        if idx < len(positions):
            lineno, _end, col, _endcol = positions[idx]
            key = (code.co_filename, lineno, col)
            if key in self.attempts:
                return self.attempts[key]
        if self.skipped:
            return None
        raise TraceError(
            f"attempt() at {code.co_filename}:{frame.f_lineno} must be the subject of a `match` statement "
            "directly in the workflow body: `match attempt(step, ...):`"
        )


def lint(fn) -> LintInfo:
    try:
        filename = inspect.getsourcefile(fn)
        if filename is None:
            raise OSError
        with open(filename, encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename)
    except (OSError, TypeError, SyntaxError):
        warnings.warn(
            f"source of workflow {fn.__qualname__} isn't available; skipping lint (attempt() then "
            "catches every error)",
            stacklevel=3,
        )
        return LintInfo(skipped=True)
    node = _find_def(tree, fn)
    if node is None:
        warnings.warn(f"couldn't locate workflow {fn.__qualname__} in its source; skipping lint", stacklevel=3)
        return LintInfo(skipped=True)
    info = LintInfo()
    _Linter(fn, filename, info).check_function(node)
    return info


def _find_def(tree: ast.AST, fn) -> ast.FunctionDef | None:
    first = fn.__code__.co_firstlineno
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == fn.__name__:
            start = min([d.lineno for d in n.decorator_list] + [n.lineno])
            if start == first or n.lineno == first:
                return n  # type: ignore[return-value]
    return None


class _Linter(ast.NodeVisitor):
    def __init__(self, fn, filename: str, info: LintInfo):
        self.fn = fn
        self.filename = filename
        self.info = info
        self.match_subjects: set[int] = set()

    def err(self, node: ast.AST, msg: str) -> LintError:
        return LintError(f"{self.filename}:{getattr(node, 'lineno', '?')}: {msg}")

    def resolve(self, expr: ast.expr):
        """Evaluate a dotted name in the workflow's globals (only Name/Attribute nodes)."""
        n = expr
        while isinstance(n, ast.Attribute):
            n = n.value
        if not isinstance(n, ast.Name):
            return None
        try:
            return eval(compile(ast.Expression(expr), self.filename, "eval"), self.fn.__globals__)
        except Exception:
            return None

    def check_function(self, node: ast.FunctionDef) -> None:
        if isinstance(node, ast.AsyncFunctionDef):
            raise self.err(node, "workflow functions can't be async")
        body = node.body
        for i, stmt in enumerate(body):
            last = i == len(body) - 1
            if isinstance(stmt, ast.Return) and not last:
                raise self.err(stmt, "return must be the last statement of a workflow")
        for stmt in body:
            self.visit_stmt_tree(stmt, top=True)

    def visit_stmt_tree(self, stmt: ast.AST, top: bool) -> None:
        for n in ast.walk(stmt):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
                continue
            if isinstance(n, ast.Return) and not top:
                pass
            self.check_node(n)
        # returns nested inside blocks of the workflow function (not in nested defs)
        for n in _walk_no_defs(stmt):
            if isinstance(n, ast.Return) and n is not stmt:
                raise self.err(n, "return must be the last statement of a workflow, not inside a block")

    def check_node(self, n: ast.AST) -> None:
        if isinstance(n, (ast.Try, ast.TryStar)):
            raise self.err(n, "try/except can't be traced; use `match attempt(...)` with Ok/Err cases")
        if isinstance(n, ast.While):
            raise self.err(n, "while loops can't be traced; use each(...) or move the loop into a step")
        if isinstance(n, ast.Compare) and any(isinstance(op, (ast.Is, ast.IsNot)) for op in n.ops):
            raise self.err(
                n, "`is`/`is not` can't be traced (a workflow value is never None); use is_null(x)"
            )
        if isinstance(n, ast.Match):
            self.check_match(n)
        if isinstance(n, ast.Call) and self.is_attempt(n.func) and id(n) not in self.match_subjects:
            raise self.err(n, "attempt(...) must be the subject of a match: `match attempt(step, ...):`")

    def is_attempt(self, func: ast.expr) -> bool:
        from sfnkit.blocks import attempt

        return self.resolve(func) is attempt

    def check_match(self, m: ast.Match) -> None:
        from sfnkit.blocks import Err, Ok

        for case in m.cases:
            for p in ast.walk(case.pattern):
                if isinstance(p, ast.MatchSingleton):
                    raise self.err(
                        p,
                        "`case None/True/False` compares by identity and would never match a workflow "
                        "value; use `case _ if is_null(x)` or compare with ==",
                    )
                if isinstance(p, (ast.MatchSequence, ast.MatchMapping, ast.MatchStar)):
                    raise self.err(p, "sequence and mapping patterns can't be traced; match on a field instead")
                if isinstance(p, ast.MatchClass):
                    cls = self.resolve(p.cls)
                    if cls not in (Ok, Err):
                        raise self.err(p, "only Ok(...) and Err(...) class patterns are supported")
                    if cls is Err:
                        self.check_err_slot(p)
        subj = m.subject
        if isinstance(subj, ast.Call) and self.is_attempt(subj.func):
            self.match_subjects.add(id(subj))
            self.info.attempts[(self.filename, subj.lineno, subj.col_offset)] = self.handled(m)

    def err_slot(self, p: ast.MatchClass) -> ast.pattern | None:
        if p.patterns:
            return p.patterns[0]
        for k, v in zip(p.kwd_attrs, p.kwd_patterns):
            if k == "error":
                return v
        return None

    def check_err_slot(self, p: ast.MatchClass) -> None:
        slot = self.err_slot(p)
        if isinstance(slot, ast.MatchAs) and slot.pattern is None and slot.name:
            import builtins

            obj = self.fn.__globals__.get(slot.name, getattr(builtins, slot.name, None))
            if isinstance(obj, type) and issubclass(obj, BaseException):
                raise self.err(
                    slot,
                    f"`Err({slot.name}, ...)` would capture any error into a new variable {slot.name!r}, "
                    f"not match the {slot.name} exception. Write a dotted name (module.{slot.name}) or the "
                    f"string {slot.name!r}",
                )

    def handled(self, m: ast.Match) -> list[str] | None:
        from sfnkit.blocks import Err

        names: list[str] = []
        guarded = False
        for case in m.cases:
            pat = case.pattern
            if isinstance(pat, ast.MatchAs) and pat.pattern is None:
                if case.guard is None:
                    return None
                guarded = True
                continue
            if isinstance(pat, ast.MatchClass) and self.resolve(pat.cls) is Err:
                slot = self.err_slot(pat)
                if slot is None or (isinstance(slot, ast.MatchAs) and slot.pattern is None):
                    if case.guard is None:
                        return None
                    guarded = True
                    continue
                names.extend(self.error_names(slot))
                if case.guard is not None:
                    guarded = True
        if guarded:
            raise self.err(
                m,
                "an Err case with a guard needs a catch-all after it (`case Err(error, cause):`), because "
                "an error whose guard fails would otherwise be silently swallowed",
            )
        return sorted(set(names))

    def error_names(self, slot: ast.pattern) -> list[str]:
        if isinstance(slot, ast.MatchOr):
            out: list[str] = []
            for p in slot.patterns:
                out.extend(self.error_names(p))
            return out
        if isinstance(slot, ast.MatchValue):
            v = slot.value
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                return [v.value]
            obj = self.resolve(v)
            if isinstance(obj, type) and issubclass(obj, BaseException):
                return [obj.__name__]
            if isinstance(obj, str):
                return [obj]
            raise self.err(slot, "an Err error pattern must be a string or an exception class")
        raise self.err(slot, "an Err error pattern must be a string, an exception class, or `_`")


def _walk_no_defs(node: ast.AST):
    stack = list(ast.iter_child_nodes(node))
    while stack:
        n = stack.pop()
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue
        yield n
        stack.extend(ast.iter_child_nodes(n))
