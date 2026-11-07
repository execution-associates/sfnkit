"""Tracing, merging and the local runner: every path of every construct."""

from __future__ import annotations

import importlib
import sys
import textwrap

import pytest

from sfnkit import LintError, TraceError, WorkflowFailed
from sfnkit.ir import ChoiceN, dump
from tests.core import flows as f


def test_straight_line():
    assert f.simple.local(3) == 9
    assert f.simple(3) == 9  # calling a workflow outside a trace runs it locally


def test_step_called_directly_is_plain_python():
    assert f.double(4) == 8


@pytest.mark.parametrize("x,z", [(1, 5), (3, 8), (7, 15)])
def test_if_elif_else(x, z):
    assert f.branchy.local(x) == {"z": z, "y": 2 * x}


def test_elif_chain_flattens_into_one_choice():
    g = f.branchy.graph()
    choices = [s for s in g.root.stmts if isinstance(s, ChoiceN)]
    assert len(choices) == 1 and len(choices[0].rules) == 2 and len(choices[0].branches) == 3


@pytest.mark.parametrize(
    "tier,value,out",
    [("gold", 0, 2), ("silver", 0, 2), ("review", 0.9, 4)],
)
def test_match_or_pattern_and_guard(tier, value, out):
    assert f.matcher.local(tier, value) == out


@pytest.mark.parametrize("tier,value", [("review", 0.1), ("other", 0.9)])
def test_match_wildcard_fails(tier, value):
    with pytest.raises(WorkflowFailed) as e:
        f.matcher.local(tier, value)
    assert e.value.error == "LowScore"


@pytest.mark.parametrize(
    "n,out",
    [
        (3, {"receipt": "r-o"}),
        (2, {"refunded": "o", "why": "PaymentDeclined"}),  # dotted exception class in the pattern
        (1, {"refunded": "o", "why": "TimeoutError"}),  # string in the pattern
        (5, None),  # catch-all
    ],
)
def test_attempt_ok_named_and_catch_all(n, out):
    assert f.catcher.local(n) == out


def test_attempt_without_catch_all_reraises_unhandled():
    assert f.narrow_catch.local(2) == {"declined": True}
    with pytest.raises(TimeoutError):
        f.narrow_catch.local(1)
    assert f.narrow_catch.graph().root.stmts[0].handled == ["PaymentDeclined"]


def test_attempt_over_a_multi_step_function():
    assert f.block_attempt.local(3) == {"sent": {"receipt": "r-o"}}
    assert f.block_attempt.local(2) == {"error": "PaymentDeclined"}


def test_maps_subworkflow_and_wait():
    out = f.mapper.local("u")
    assert out["audit"] == {"count": 3}
    assert [r["label"] for r in out["enriched"]] == ["run", "run", "run"]


def test_reusable_child_workflow():
    assert f.parent_wf.local(2) == {"a": 4, "b": [2, 4, 6]}


def test_pipeline_paths():
    assert f.pipeline.local("a" * 20) == {"receipt": {"receipt": "r-published"}, "audit": {"count": 3}}
    with pytest.raises(WorkflowFailed):
        f.pipeline.local("u")


def test_values_merge_after_branches_without_extra_phis():
    text = dump(f.pipeline.graph())
    assert text.count(" = b0:") == 2  # order and receipt; flattening leftovers are pruned


# trace-time errors


def _module(tmp_path, name: str, body: str):
    (tmp_path / f"{name}.py").write_text(textwrap.dedent(body))
    sys.path.insert(0, str(tmp_path))
    try:
        sys.modules.pop(name, None)
        return importlib.import_module(name)
    finally:
        sys.path.remove(str(tmp_path))


HEADER = """
from sfnkit import step, workflow, attempt, Ok, Err, each, is_null, length
@step
def s(x: int) -> int:
    return x
"""


@pytest.mark.parametrize(
    "body,msg",
    [
        ("    if s(x) is None:\n        return 1\n    return 2\n", "is"),
        ("    match s(x):\n        case None:\n            y = 1\n    return y\n", "None/True/False"),
        ("    match s(x):\n        case [a, b]:\n            y = 1\n    return y\n", "sequence"),
        ("    match s(x):\n        case {'a': 1}:\n            y = 1\n    return y\n", "sequence and mapping"),
        ("    try:\n        y = s(x)\n    except Exception:\n        y = 0\n    return y\n", "attempt"),
        ("    while x:\n        x = s(x)\n    return x\n", "while"),
        ("    if x:\n        return s(x)\n    return 1\n", "last statement"),
        ("    o = attempt(s, x)\n    return 1\n", "subject of a match"),
        (
            "    match attempt(s, x):\n        case Ok(v):\n            y = v\n"
            "        case Err(e, c) if e == 'A':\n            y = 0\n    return y\n",
            "catch-all",
        ),
        (
            "    match attempt(s, x):\n        case Ok(v):\n            y = v\n"
            "        case Err(ValueError, c):\n            y = 0\n    return y\n",
            "capture any error",
        ),
    ],
)
def test_lint_rejects(tmp_path, body, msg):
    src = HEADER + "@workflow\ndef w(x: int):\n" + body
    with pytest.raises(LintError, match=msg):
        _module(tmp_path, f"lint_mod_{abs(hash(body))}", src)


@pytest.mark.parametrize(
    "body,msg",
    [
        ("    return s(x) + 1\n", "arithmetic"),
        ("    for v in s(x):\n        s(v)\n    return 1\n", "iterate"),
        ("    return len(s(x))\n", "length"),
        (
            "    with each([1, 2]) as it:\n        y = s(it.item)\n        it.result(y)\n    return y\n",
            "each block",
        ),
        (
            "    for v in [1, 2]:\n        if s(v) > 1:\n            s(v)\n    return 1\n",
            "more than once",
        ),
        ("    if s(x) > 1:\n        y = s(1)\n    return y\n", "UnboundLocalError"),
    ],
)
def test_trace_errors(tmp_path, body, msg):
    src = HEADER + "@workflow\ndef w(x: int):\n" + body
    mod = _module(tmp_path, f"trace_mod_{abs(hash(body))}", src)
    with pytest.raises(TraceError, match=msg):
        mod.w.graph()


def test_unrolled_python_loop_over_literals_is_fine(tmp_path):
    src = HEADER + "@workflow\ndef w(x: int):\n    out = []\n    for v in [1, 2, 3]:\n        out.append(s(v))\n    return out\n"
    mod = _module(tmp_path, "loop_ok", src)
    assert mod.w.local(0) == [1, 2, 3]


def test_repeated_condition_is_memoized(tmp_path):
    src = HEADER + (
        "@workflow\ndef w(x: int):\n    y = s(x)\n    if y > 1:\n        a = s(1)\n    else:\n        a = s(2)\n"
        "    if y > 1:\n        b = s(a)\n    else:\n        b = s(a)\n    return b\n"
    )
    mod = _module(tmp_path, "memo", src)
    assert mod.w.local(5) == 1 and mod.w.local(0) == 2


def test_and_or_short_circuit(tmp_path):
    src = HEADER + (
        "@workflow\ndef w(x: int, y: int):\n    a = s(x)\n    b = s(y)\n"
        "    if a > 1 and b > 1:\n        r = s(1)\n    elif a > 1 or b > 1:\n        r = s(2)\n    else:\n        r = s(3)\n"
        "    return r\n"
    )
    mod = _module(tmp_path, "andor", src)
    assert [mod.w.local(2, 2), mod.w.local(2, 0), mod.w.local(0, 2), mod.w.local(0, 0)] == [1, 2, 2, 3]


def test_path_cap(tmp_path):
    conds = "".join(f"    if s({i}) > x:\n        s({i})\n" for i in range(5))
    src = HEADER + "@workflow(max_paths=8)\ndef w(x: int):\n" + conds + "    return 1\n"
    mod = _module(tmp_path, "cap", src)
    with pytest.raises(TraceError, match="more than 8 paths"):
        mod.w.graph()
