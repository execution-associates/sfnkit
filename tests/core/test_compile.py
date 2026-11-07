"""Compiled ASL shape, IAM resolution, golden files and the compute-backend extension point."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from sfnkit import Allow, ComputeSpec, Policy, step, workflow
from sfnkit.compile.app import build_app
from sfnkit.compute.base import PAYLOAD, ComputeBackend, TaskState, register_backend
from tests.core import flows as f

GOLDEN = Path(__file__).parent / "golden"
FLOWS = ["simple", "branchy", "matcher", "catcher", "narrow_catch", "block_attempt", "mapper", "pipeline", "parent_wf"]


def states(defn: dict):
    """Every state in a definition, including nested branches and map processors."""
    for name, s in defn["States"].items():
        yield name, s
        for b in s.get("Branches", []):
            yield from states(b)
        if "ItemProcessor" in s:
            yield from states(s["ItemProcessor"])


def by_type(defn, t):
    return [s for _, s in states(defn) if s["Type"] == t]


def test_every_definition_is_jsonata_and_terminates():
    for name in FLOWS:
        app = build_app(getattr(f, name))
        for sm in app.state_machines:
            d = sm.definition
            assert d["QueryLanguage"] == "JSONata"
            ends = [s for _, s in states(d) if s.get("End") or s["Type"] == "Fail"]
            assert ends, f"{name}/{sm.name} has no terminal state"
            text = json.dumps(d)
            for ph in __import__("re").findall(r"\$\{([A-Za-z0-9]+)\}", text):
                assert ph in sm.substitutions, f"{ph} has no substitution in {name}/{sm.name}"


def test_independent_steps_share_a_parallel_level():
    d = build_app(f.pipeline).root.definition
    par = by_type(d, "Parallel")
    assert par and len(par[0]["Branches"]) == 2  # the inline and the distributed map over docs


def test_maps_inline_and_distributed():
    d = build_app(f.pipeline).root.definition
    maps = by_type(d, "Map")
    modes = sorted(m["ItemProcessor"]["ProcessorConfig"]["Mode"] for m in maps)
    assert modes == ["DISTRIBUTED", "INLINE"]
    dist = next(m for m in maps if m["ItemProcessor"]["ProcessorConfig"]["Mode"] == "DISTRIBUTED")
    assert "ItemReader" in dist and "ResultWriter" in dist
    assert dist["MaxConcurrency"] == 50


def test_attempt_catches_only_handled_names():
    d = build_app(f.narrow_catch).root.definition
    task = next(s for _, s in states(d) if "Catch" in s)
    assert task["Catch"][0]["ErrorEquals"] == ["PaymentDeclined"]
    d = build_app(f.catcher).root.definition
    task = next(s for _, s in states(d) if "Catch" in s)
    assert task["Catch"][0]["ErrorEquals"] == ["States.ALL"]


def test_multi_step_attempt_is_a_parallel_with_catch():
    d = build_app(f.block_attempt).root.definition
    par = next(s for _, s in states(d) if s["Type"] == "Parallel" and "Catch" in s)
    assert len(par["Branches"]) == 1


def test_children_are_separate_state_machines():
    app = build_app(f.parent_wf)
    assert [sm.name for sm in app.state_machines] == ["main", "child-wf"]
    root = app.root
    assert [n.child for n in root.needs if n.kind == "start_child"] == ["child-wf"]
    starts = [s for _, s in states(root.definition) if s.get("Resource", "").startswith("arn:aws:states:::states")]
    assert len(starts) == 2  # direct call + the call inside the map
    app = build_app(f.mapper)
    assert [sm.name for sm in app.state_machines] == ["main", "scoring"]


def test_retry_and_transient_errors():
    d = build_app(f.pipeline).root.definition
    parse = next(s for n, s in states(d) if n.startswith("parse_"))
    assert parse["Retry"][0]["ErrorEquals"] == ["ValueError"]
    assert "Lambda.ServiceException" in parse["Retry"][1]["ErrorEquals"]


def test_facts_requested_for_conditions():
    d = build_app(f.pipeline).root.definition
    score = build_app(f.pipeline).state_machines[1].definition
    payload = next(s for n, s in states(score) if n.startswith("score_rows"))["Arguments"]["Payload"]
    assert '"facts"' in payload and '"tier"' in payload and '"value"' in payload
    choice = by_type(d, "Choice")[0]
    assert ".f.k_" in choice["Choices"][0]["Condition"]


def test_images_follow_deps():
    app = build_app(f.pipeline)
    deps = sorted(i.deps for i in app.images)
    assert deps == [(), ("httpx",), ("pyyaml",)]
    fetch = next(fn for fn in app.functions if fn.step.name == "fetch")
    assert fetch.compute.memory == 1024


# IAM


def test_iam_inherit_extend_replace():
    app = build_app(f.pipeline)
    pol = {fn.step.name: fn.policy for fn in app.functions}
    assert pol["fetch"] == f.base  # inherits the workflow base set
    assert pol["parse"] == f.base + Allow("dynamodb:PutItem", "*")  # base + extra
    assert pol["notify"] == Policy(Allow("sqs:SendMessage", "*"))  # own only, no base set
    assert f.base.statements[0] not in pol["notify"].statements


def test_iam_no_workflow_base_means_empty():
    app = build_app(f.simple)
    assert all(not fn.policy for fn in app.functions)


iam_a = Policy(Allow("s3:GetObject", "arn:aws:s3:::a/*"))
iam_b = Policy(Allow("s3:GetObject", "arn:aws:s3:::b/*"))


@step
def shared(x: int) -> int:
    return x


@workflow(iam=iam_a)
def two_policies(x: int):
    from sfnkit import subworkflow

    a = shared(x)
    with subworkflow("other", iam=iam_b):
        b = shared(a)
    return b


def test_same_step_under_two_policies_gets_two_functions():
    app = build_app(two_policies)
    fns = [fn for fn in app.functions if fn.step.name == "shared"]
    assert len(fns) == 2 and {fn.policy for fn in fns} == {iam_a, iam_b}
    assert len({fn.logical_id for fn in fns}) == 2


# a second compute backend plugs in without touching the core


@dataclass(eq=False)
class FakeContainer(ComputeSpec):
    kind = "fake"
    cpu: int = 256


class FakeBackend(ComputeBackend):
    spec_type = FakeContainer
    assume_role_service = "ecs-tasks.amazonaws.com"

    def resources(self, fn, ctx):
        return {fn.logical_id: {"Type": "AWS::ECS::TaskDefinition", "Properties": {"Cpu": str(fn.compute.cpu)}}}

    def baseline_statements(self, fn, ctx):
        return []

    def task_state(self, fn):
        return TaskState(
            resource="arn:aws:states:::ecs:runTask.waitForTaskToken",
            arguments={"TaskDefinition": f"${{{fn.logical_id}Arn}}", "Overrides": {"Env": PAYLOAD}},
            result="$states.result",
            substitutions={f"{fn.logical_id}Arn": {"Ref": fn.logical_id}},
        )

    def state_machine_statements(self, fn):
        return [{"Effect": "Allow", "Action": "ecs:RunTask", "Resource": {"Ref": fn.logical_id}}]


register_backend(FakeBackend)


@step(compute=FakeContainer(cpu=1024))
def heavy(x: int) -> int:
    return x


@workflow
def mixed(x: int):
    return heavy(f.double(x))


def test_second_backend_needs_no_core_changes():
    app = build_app(mixed)
    tasks = by_type(app.root.definition, "Task")
    assert {t["Resource"] for t in tasks} == {"arn:aws:states:::lambda:invoke", "arn:aws:states:::ecs:runTask.waitForTaskToken"}
    ecs = next(t for t in tasks if "ecs" in t["Resource"])
    assert ecs["Arguments"]["Overrides"]["Env"].startswith("{% ")
    from sfnkit.compile.cfn import main_template

    tmpl = main_template(app)
    assert any(r["Type"] == "AWS::ECS::TaskDefinition" for r in tmpl["Resources"].values())


# golden files: review diffs when the compiler changes; refresh with SFNKIT_UPDATE_GOLDEN=1


@pytest.mark.parametrize("name", FLOWS)
def test_golden(name):
    app = build_app(getattr(f, name))
    got = {sm.name: sm.definition for sm in app.state_machines}
    path = GOLDEN / f"{name}.json"
    if os.environ.get("SFNKIT_UPDATE_GOLDEN") or not path.exists():
        GOLDEN.mkdir(exist_ok=True)
        path.write_text(json.dumps(got, indent=1, sort_keys=True) + "\n")
    assert json.loads(path.read_text()) == json.loads(json.dumps(got))
