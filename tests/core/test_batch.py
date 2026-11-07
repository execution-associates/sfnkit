"""AWS Batch: compiled ASL, CloudFormation, images, and the container runtime's array mode."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from examples import batch as ex
from sfnkit import Batch, BatchQueue, CompileError, FargateQueue, each, step, workflow
from sfnkit.build.dockerfile import render_dockerfile
from sfnkit.compile.app import build_app
from sfnkit.compile.cfn import main_template
from sfnkit.runtime import container
from sfnkit.runtime import protocol as P
from sfnkit.runtime.results import resolve

GOLDEN = Path(__file__).parent / "golden"
SHARED = BatchQueue("arn:aws:batch:us-west-2:123456789012:job-queue/shared")


@step(compute=Batch(queue=SHARED, vcpus=1, memory=2048))
def crunch(x: int) -> int:
    return x


@workflow
def uses_shared(x: int):
    return crunch(x)


@workflow(type="EXPRESS")
def fast(n: int):
    return ex.total([n])


@workflow
def fan(items: list):
    with each(items, distributed=True, child_type="EXPRESS") as it:
        it.result(ex.picky(it.item))
    return it.value


@workflow
def per_sample(items: list):
    return ex.picky.map(items, distributed=True)


def states(defn: dict):
    for name, s in defn["States"].items():
        yield name, s
        for b in s.get("Branches", []):
            yield from states(b)
        if "ItemProcessor" in s:
            yield from states(s["ItemProcessor"])


def by_name(defn: dict) -> dict:
    return dict(states(defn))


# ------------------------------------------------------------------------------------------------- ASL


def test_single_job_passes_envelope_and_token_and_returns_what_the_container_sends():
    s = by_name(build_app(ex.batchy).root.definition)["total_n11"]
    assert s["Resource"] == "arn:aws:states:::batch:submitJob.sync"
    env = {e["Name"]: e["Value"] for e in s["Arguments"]["ContainerOverrides"]["Environment"]}
    assert env[P.ENV_ENVELOPE].startswith("{% $string({") and '"node": "n11"' in env[P.ENV_ENVELOPE]
    assert env[P.ENV_TOKEN] == "{% $states.context.Task.Token %}"
    assert "ArrayProperties" not in s["Arguments"]
    assert "TimeoutSeconds" not in s  # queue time is unbounded; the job definition bounds run time
    assert s["Assign"] == {"n11": "{% $states.result %}"}


def test_map_on_a_batch_step_is_one_array_job():
    d = by_name(build_app(ex.batchy).root.definition)
    assert not any(s["Type"] == "Map" for s in d.values())
    sq = d["square_m2"]
    args = sq["Arguments"]
    assert args["ArrayProperties"]["Size"].startswith("{% $max([2, $ceil(")
    envelope = next(
        e["Value"] for e in args["ContainerOverrides"]["Environment"] if e["Name"] == P.ENV_ENVELOPE
    )
    assert '"array": {"param": "item", "items": $n1.it.k_' in envelope and '"per": 1' in envelope
    assert '"fail_at": $input.fail_at' in envelope  # other arguments are shared by every child
    # an empty list skips the job, and an oversized one fails with a clear error
    check = d["Check_m2"]
    assert [c["Next"] for c in check["Choices"]] == ["Empty_m2", "TooBig_m2"] and check[
        "Default"
    ] == "square_m2"
    assert d["TooBig_m2"]["Error"] == "sfnkit.ArrayTooLarge"


def test_array_over_array_results_and_items_per_job():
    d = by_name(build_app(ex.batchy).root.definition)
    s = d["plus_one_m5"]
    assert "$ceil($m2.arr.n / 10)" in s["Arguments"]["ArrayProperties"]["Size"]
    envelope = s["Arguments"]["ContainerOverrides"]["Environment"][0]["Value"]
    assert '"items": $m2.arr' in envelope and '"per": 10' in envelope


def test_literal_items_are_written_to_s3_first():
    d = by_name(build_app(ex.batchy).root.definition)
    put = d["PutItems_m8"]
    assert put["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert put["Next"] == "Check_m8"


def test_producer_writes_the_items_file_with_a_count():
    s = by_name(build_app(ex.batchy).root.definition)["make_items_n1"]
    assert '"mode": "s3"' in s["Arguments"]["Payload"]


def test_length_of_array_results_reads_the_count():
    d = by_name(build_app(ex.batchy).root.definition)
    conditions = [c["Condition"] for s in d.values() if s["Type"] == "Choice" for c in s["Choices"]]
    assert "{% ($m2.arr.n > 50) %}" in conditions


def test_batch_steps_are_rejected_in_express():
    with pytest.raises(CompileError, match="STANDARD"):
        build_app(fast)


def test_batch_steps_are_rejected_in_express_map_children():
    with pytest.raises(CompileError, match="EXPRESS Distributed Map child"):
        build_app(fan)


def test_distributed_map_of_batch_steps_runs_single_jobs():
    d = by_name(build_app(per_sample).root.definition)
    assert any(s["Type"] == "Map" for s in d.values())
    job = next(s for s in d.values() if s.get("Resource") == "arn:aws:states:::batch:submitJob.sync")
    assert "ArrayProperties" not in job["Arguments"]


def test_golden():
    """Snapshot of the compiled ASL, which tests/test_aws_validate.py also checks against AWS."""
    got = {sm.name: sm.definition for sm in build_app(ex.batchy).state_machines}
    path = GOLDEN / "batch.json"
    if os.environ.get("SFNKIT_UPDATE_GOLDEN") or not path.exists():
        path.write_text(json.dumps(got, indent=1, sort_keys=True) + "\n")
    assert json.loads(path.read_text()) == json.loads(json.dumps(got))


# --------------------------------------------------------------------------------------- CloudFormation


def _lint_errors(template: dict) -> list[str]:
    from cfnlint.api import ManualArgs, lint

    matches = lint(json.dumps(template), config=ManualArgs(regions=["us-west-2"]))
    return [f"{m.rule.id} {m.message}" for m in matches if m.rule.id.startswith("E")]


def test_template_lints_clean_and_shares_one_queue():
    tpl = main_template(build_app(ex.batchy))
    assert _lint_errors(tpl) == []
    res = tpl["Resources"]
    kinds = [r["Type"] for r in res.values()]
    assert kinds.count("AWS::Batch::JobDefinition") == 5
    assert kinds.count("AWS::Batch::ComputeEnvironment") == 1 and kinds.count("AWS::Batch::JobQueue") == 1
    env = next(r for r in res.values() if r["Type"] == "AWS::Batch::ComputeEnvironment")
    assert env["Properties"]["ComputeResources"]["Type"] == "FARGATE_SPOT"


def test_job_definition_roles_and_retries():
    app = build_app(ex.batchy)
    res = main_template(app)["Resources"]
    fn = next(f for f in app.functions if f.step.name == "picky")
    jd = res[fn.logical_id]["Properties"]
    c = jd["ContainerProperties"]
    assert c["JobRoleArn"] == {"Fn::GetAtt": [f"{fn.logical_id}Role", "Arn"]}
    assert {"Type": "VCPU", "Value": "0.25"} in c["ResourceRequirements"]
    assert c["NetworkConfiguration"] == {"AssignPublicIp": "ENABLED"}
    rules = jd["RetryStrategy"]["EvaluateOnExit"]
    assert rules[0] == {"OnExitCode": "1", "Action": "EXIT"}  # a reported step failure isn't re-run by Batch
    assert all(r["Action"] == "EXIT" for r in rules)  # anything else (pull errors, Spot, lost hosts) retries
    assert not any(r.get("OnReason") == "*" or r.get("OnStatusReason") == "*" for r in rules)
    role = res[f"{fn.logical_id}Role"]["Properties"]
    assert role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "ecs-tasks.amazonaws.com"
    }
    stmts = [s for p in role["Policies"] for s in p["PolicyDocument"]["Statement"]]
    assert any("states:SendTaskSuccess" in s["Action"] for s in stmts)


def test_state_machine_role_can_submit_and_track_jobs():
    tpl = main_template(build_app(ex.batchy))
    sm_role = tpl["Resources"]["SmMainRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    actions = {a for s in sm_role for a in ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])}
    assert {"batch:SubmitJob", "batch:DescribeJobs", "events:PutRule", "s3:PutObject"} <= actions


def test_existing_queue_is_referenced_not_created():
    app = build_app(uses_shared)
    res = main_template(app)["Resources"]
    assert not any(
        r["Type"] in ("AWS::Batch::ComputeEnvironment", "AWS::Batch::JobQueue") for r in res.values()
    )
    task = next(s for _, s in states(app.root.definition) if s["Type"] == "Task")
    assert task["Arguments"]["JobQueue"] == SHARED.arn


def test_spec_validation():
    q = FargateQueue(subnets=["subnet-1"], security_groups=["sg-1"])
    with pytest.raises(ValueError, match="vcpus"):
        Batch(queue=q, vcpus=3)
    with pytest.raises(ValueError, match="GPU"):
        Batch(queue=q, gpus=1)
    with pytest.raises(ValueError, match="subnet"):
        FargateQueue(subnets=[], security_groups=["sg-1"])
    Batch(queue=BatchQueue("arn:q", platform="EC2"), vcpus=3, gpus=1)  # EC2 takes any size and GPUs


# ---------------------------------------------------------------------------------------------- images


def test_batch_images_start_the_container_runtime():
    app = build_app(ex.batchy)
    runtimes = {img.runtime for img in app.images}
    assert runtimes == {"lambda", "container"}  # same deps, different images
    img = next(i for i in app.images if i.runtime == "container")
    df = render_dockerfile(img)
    assert 'ENTRYPOINT ["/var/lang/bin/python3", "-m", "sfnkit.runtime.container"]' in df
    assert "lambda_handler" not in df


# ------------------------------------------------------------------------------------- container runtime


class FakeSfn:
    def __init__(self, closed: bool = False):
        self.calls: list[tuple[str, dict]] = []
        self.closed = closed

    def _call(self, kind, kw):
        if self.closed:
            err = Exception("closed")
            err.response = {"Error": {"Code": "TaskTimedOut"}}  # type: ignore[attr-defined]
            raise err
        self.calls.append((kind, kw))

    def send_task_success(self, **kw):
        self._call("success", kw)

    def send_task_failure(self, **kw):
        self._call("failure", kw)


class FakeBatch:
    def __init__(self):
        self.terminated: list[str] = []

    def terminate_job(self, jobId, reason):
        self.terminated.append(jobId)


@pytest.fixture
def s3(monkeypatch):
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        import boto3

        client = boto3.client("s3")
        client.create_bucket(Bucket="art")
        yield client


def _env(envelope: dict, **extra) -> dict:
    return {
        P.ENV_ENVELOPE: json.dumps(envelope),
        P.ENV_TOKEN: "tok",
        P.ENV_BUCKET: "art",
        P.ENV_MODULE: "examples.batch",
        **extra,
    }


def _array_envelope(locator: dict, per: int = 1, fail_at: int = -1) -> dict:
    return {
        "sw": 1,
        "step": "examples.batch.square",
        "execution": "ex1",
        "node": "m2",
        "args": {"fail_at": fail_at},
        "array": {"param": "item", "items": locator, "per": per},
    }


def _put_items(s3, items: list) -> dict:
    s3.put_object(Bucket="art", Key="runs/ex1/n1/items.json", Body=json.dumps(items))
    return {"bucket": "art", "key": "runs/ex1/n1/items.json", "n": len(items)}


def test_array_child_runs_its_slice_and_writes_results(s3):
    loc = _put_items(s3, [{"$sw": 1, "v": {"i": i}} for i in range(5)])
    sfn = FakeSfn()
    env = _env(_array_envelope(loc, per=2), AWS_BATCH_JOB_ARRAY_INDEX="1", AWS_BATCH_JOB_ID="j:1")
    assert container.main(environ=env, sfn=sfn, s3=s3) == 0
    assert sfn.calls == []  # success needs no callback: the array job's completion ends the task
    keys = sorted(
        o["Key"] for o in s3.list_objects_v2(Bucket="art", Prefix="runs/ex1/m2/results/")["Contents"]
    )
    assert keys == ["runs/ex1/m2/results/2.json", "runs/ex1/m2/results/3.json"]
    out = json.loads(s3.get_object(Bucket="art", Key=keys[0])["Body"].read())
    assert resolve(out, s3)["sq"] == 4


def test_padding_child_past_the_end_does_nothing(s3):
    loc = _put_items(s3, [{"i": 0}])  # one item still makes an array of two children
    env = _env(_array_envelope(loc), AWS_BATCH_JOB_ARRAY_INDEX="1", AWS_BATCH_JOB_ID="j:1")
    assert container.main(environ=env, sfn=FakeSfn(), s3=s3) == 0
    assert "Contents" not in s3.list_objects_v2(Bucket="art", Prefix="runs/ex1/m2/results/")


def test_failing_child_fails_the_task_by_name_and_stops_the_array(s3):
    loc = _put_items(s3, [{"i": i} for i in range(3)])
    sfn, batch = FakeSfn(), FakeBatch()
    env = _env(_array_envelope(loc, fail_at=2), AWS_BATCH_JOB_ARRAY_INDEX="2", AWS_BATCH_JOB_ID="parent-1:2")
    assert container.main(environ=env, sfn=sfn, s3=s3, batch=batch) == 1
    kind, kw = sfn.calls[0]
    assert kind == "failure" and kw["error"] == "LookupError"
    assert json.loads(kw["cause"])["errorMessage"] == "item 2 failed on purpose"
    assert batch.terminated == ["parent-1"]


def test_results_read_back_in_order_from_per_item_files(s3):
    for i in range(12):
        s3.put_object(
            Bucket="art", Key=P.results_key("ex1", "m2", i), Body=json.dumps({"$sw": 1, "v": i * i})
        )
    wrapper = {"$sw": 1, "arr": {"b": "art", "p": "runs/ex1/m2/results/", "n": 12}}
    assert resolve(wrapper, s3) == [i * i for i in range(12)]
    assert resolve({"$sw": 1, "arr": {"b": "art", "p": "x/", "n": 0}}, s3) == []


def test_array_over_array_results(s3):
    for i in range(3):
        s3.put_object(
            Bucket="art",
            Key=P.results_key("ex1", "m2", i),
            Body=json.dumps({"$sw": 1, "v": {"i": i, "sq": i * i}}),
        )
    env_ = {
        "sw": 1,
        "step": "examples.batch.plus_one",
        "execution": "ex1",
        "node": "m5",
        "args": {},
        "array": {"param": "r", "items": {"b": "art", "p": "runs/ex1/m2/results/", "n": 3}, "per": 10},
    }
    assert container.main(environ=_env(env_, AWS_BATCH_JOB_ARRAY_INDEX="0"), sfn=FakeSfn(), s3=s3) == 0
    wrapper = {"$sw": 1, "arr": {"b": "art", "p": "runs/ex1/m5/results/", "n": 3}}
    assert resolve(wrapper, s3) == [1, 2, 5]


def test_single_job_reports_success_with_the_wrapper(s3):
    env_ = {"sw": 1, "step": "examples.batch.picky", "execution": "ex1", "node": "n3", "args": {"x": 4}}
    sfn = FakeSfn()
    assert container.main(environ=_env(env_), sfn=sfn, s3=s3) == 0
    kind, kw = sfn.calls[0]
    assert kind == "success" and json.loads(kw["output"]) == {"$sw": 1, "v": 40}


def test_single_job_reports_failure_by_class_name(s3):
    env_ = {"sw": 1, "step": "examples.batch.picky", "execution": "ex1", "node": "n3", "args": {"x": -1}}
    sfn = FakeSfn()
    assert container.main(environ=_env(env_), sfn=sfn, s3=s3) == 1
    assert sfn.calls[0][0] == "failure" and sfn.calls[0][1]["error"] == "ValueError"


def test_a_closed_task_is_not_an_error(s3):
    env_ = {"sw": 1, "step": "examples.batch.picky", "execution": "ex1", "node": "n3", "args": {"x": 4}}
    assert container.main(environ=_env(env_), sfn=FakeSfn(closed=True), s3=s3) == 0


# --------------------------------------------------------------------------- failure budgets and resume


@workflow
def inline_tolerant(items: list):
    return ex.make_items.map(items, tolerated_failure_count=1)


def test_tolerated_failures_compile_into_the_array_job():
    d = by_name(build_app(ex.batchy).root.definition)
    s = next(
        s for s in d.values() if s.get("Resource", "").endswith("submitJob.sync") and "fragile" in str(s)
    )
    envelope = s["Arguments"]["ContainerOverrides"]["Environment"][0]["Value"]
    assert '"tolerate": 1' in envelope
    assert '"t": true' in next(iter(s["Assign"].values()))  # missing results read as None


def test_inline_map_with_a_failure_budget_is_rejected():
    with pytest.raises(CompileError, match="inline Map state can't tolerate failures"):
        build_app(inline_tolerant)


def _fragile_envelope(locator: dict, bad: list, tolerate: int | None) -> dict:
    arr = {"param": "item", "items": locator, "per": 3}
    if tolerate is not None:
        arr["tolerate"] = tolerate
    return {
        "sw": 1,
        "step": "examples.batch.fragile",
        "execution": "ex1",
        "node": "m9",
        "args": {"bad": bad},
        "array": arr,
    }


def _results(s3, node: str = "m9") -> list[str]:
    got = s3.list_objects_v2(Bucket="art", Prefix=f"runs/ex1/{node}/results/").get("Contents", [])
    return sorted(o["Key"].rsplit("/", 1)[1] for o in got)


def test_tolerated_failure_is_recorded_and_the_child_carries_on(s3):
    loc = _put_items(s3, [{"i": i} for i in range(3)])
    sfn = FakeSfn()
    env = _env(
        _fragile_envelope(loc, [1], tolerate=1), AWS_BATCH_JOB_ARRAY_INDEX="0", AWS_BATCH_JOB_ID="job-7:0"
    )
    assert container.main(environ=env, sfn=sfn, s3=s3) == 0 and sfn.calls == []
    assert _results(s3) == ["0.json", "2.json"]
    rec = json.loads(s3.get_object(Bucket="art", Key="runs/ex1/m9/errors/job-7/1.json")["Body"].read())
    assert rec["error"] == "ValueError" and rec["index"] == 1
    wrapper = {"$sw": 1, "arr": {"b": "art", "p": "runs/ex1/m9/results/", "n": 3, "t": True}}
    assert resolve(wrapper, s3) == [0, None, 2]


def test_going_over_the_budget_fails_the_step_and_stops_the_array(s3):
    loc = _put_items(s3, [{"i": i} for i in range(3)])
    sfn, batch = FakeSfn(), FakeBatch()
    env = _env(
        _fragile_envelope(loc, [0, 2], tolerate=1), AWS_BATCH_JOB_ARRAY_INDEX="0", AWS_BATCH_JOB_ID="job-8:0"
    )
    assert container.main(environ=env, sfn=sfn, s3=s3, batch=batch) == 1
    kind, kw = sfn.calls[0]
    assert kind == "failure" and kw["error"] == "ToleratedFailureExceeded"
    assert "item 0: ValueError: item 0 is bad" in json.loads(kw["cause"])["errorMessage"]
    assert batch.terminated == ["job-8"]


def test_a_rerun_only_runs_items_without_a_result(s3):
    loc = _put_items(s3, [{"i": i} for i in range(3)])
    s3.put_object(
        Bucket="art", Key=P.results_key("ex1", "m9", 1), Body=json.dumps({"$sw": 1, "v": "done before"})
    )
    # item 1 would fail now, but it already finished in an earlier attempt of this run, so it isn't re-run
    env = _env(
        _fragile_envelope(loc, [1], tolerate=None), AWS_BATCH_JOB_ARRAY_INDEX="0", AWS_BATCH_JOB_ID="job-9:0"
    )
    assert container.main(environ=env, sfn=FakeSfn(), s3=s3) == 0
    wrapper = {"$sw": 1, "arr": {"b": "art", "p": "runs/ex1/m9/results/", "n": 3}}
    assert resolve(wrapper, s3) == [0, "done before", 2]


def test_a_missing_result_is_an_error_without_a_budget(s3):
    with pytest.raises(Exception) as e:
        resolve({"$sw": 1, "arr": {"b": "art", "p": "runs/ex1/none/", "n": 1}}, s3)
    assert "NoSuchKey" in repr(e.value) or "404" in repr(e.value)


def test_local_runs_enforce_the_budget():
    from sfnkit.errors import ToleratedFailureExceeded

    assert ex.batchy.local(n=4, bad=[2])["summary"]["skipped"] == [2]
    with pytest.raises(ToleratedFailureExceeded):
        ex.batchy.local(n=4, bad=[1, 2])


def test_items_body_is_the_list_itself():
    put = by_name(build_app(ex.batchy).root.definition)["PutItems_m8"]
    assert put["Arguments"]["Body"] == '{% [{"i": 7}, {"i": 8}] %}'


def test_items_file_written_as_a_json_string_still_reads_as_a_list(s3):
    s3.put_object(
        Bucket="art", Key="runs/ex1/m8/items.json", Body=json.dumps(json.dumps([{"i": 7}, {"i": 8}]))
    )
    loc = {"bucket": "art", "key": "runs/ex1/m8/items.json", "n": 2}
    env = _env(_array_envelope(loc), AWS_BATCH_JOB_ARRAY_INDEX="1", AWS_BATCH_JOB_ID="j:1")
    assert container.main(environ=env, sfn=FakeSfn(), s3=s3) == 0
    out = json.loads(s3.get_object(Bucket="art", Key=P.results_key("ex1", "m2", 1))["Body"].read())
    assert resolve(out, s3)["sq"] == 64
