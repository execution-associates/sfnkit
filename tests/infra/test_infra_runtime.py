import json

import infra_dummy_steps
import pytest

from sfnkit.runtime import protocol as P
from sfnkit.runtime.execute import execute
from sfnkit.runtime.lambda_handler import handler
from sfnkit.runtime.results import parse_uri, resolve, wrap


def W(**kw):
    return {P.MARK: P.VERSION, **kw}


def env(step, args, **extra):
    return {
        "sw": 1,
        "step": f"infra_dummy_steps.{step}",
        "execution": "ex1",
        "node": "n1",
        "args": args,
        **extra,
    }


# ------------------------------------------------------------------------------------------- resolve


def test_resolve_plain_and_nested_wrappers():
    value = {"a": W(v=1), "b": [W(v=W(v=2)), 3], "c": W(x=W(v={"k": "z"}), f={"k0": 1}, i=4)}
    assert resolve(value) == {"a": 1, "b": [2, 3], "c": {"k": "z"}}


def test_resolve_projection_and_cause():
    assert resolve(W(pj=W(v={"a": [10, 20]}), p=["a", 1])) == 20
    assert resolve(W(pj={"a": {"b": 5}}, p=["a", "b"])) == 5
    assert resolve(W(pj=W(v={"a": 1}), p=["missing"])) is None  # like JSONata and the local runner
    lam = json.dumps({"errorMessage": "boom", "errorType": "ValueError"})
    assert resolve(W(c=lam)) == {"errorMessage": "boom", "errorType": "ValueError"}
    assert resolve(W(c="States.Timeout: task timed out")) == "States.Timeout: task timed out"


def test_resolve_s3(s3):
    s3.put_object(Bucket="art", Key="runs/e/n/x.json", Body=json.dumps({"big": [1, 2]}))
    assert resolve(W(s3="s3://art/runs/e/n/x.json"), s3=s3) == {"big": [1, 2]}


def test_resolve_dmap_orders_and_marks_failures(s3):
    def child(i, value):
        return {
            "Input": json.dumps({"item": i, "index": i, "free": {}}),
            "Output": json.dumps(W(x=W(v=value), i=i)),
            "Status": "SUCCEEDED",
        }

    s3.put_object(Bucket="art", Key="res/SUCCEEDED_0.json", Body=json.dumps([child(2, "c"), child(0, "a")]))
    s3.put_object(Bucket="art", Key="res/SUCCEEDED_1.json", Body=json.dumps([child(3, "d")]))
    failed = [
        {"Input": json.dumps({"item": 1, "index": 1}), "Error": "Boom", "Cause": "x", "Status": "FAILED"}
    ]
    s3.put_object(Bucket="art", Key="res/FAILED_0.json", Body=json.dumps(failed))
    manifest = {
        "DestinationBucket": "art",
        "MapRunArn": "arn:aws:states:us-west-2:1:mapRun:sm/x:y",
        "ResultFiles": {
            "SUCCEEDED": [
                {"Key": "res/SUCCEEDED_0.json", "Size": 1},
                {"Key": "res/SUCCEEDED_1.json", "Size": 1},
            ],
            "FAILED": [{"Key": "res/FAILED_0.json", "Size": 1}],
            "PENDING": [],
        },
    }
    s3.put_object(Bucket="art", Key="res/manifest.json", Body=json.dumps(manifest))
    assert resolve(W(dmap={"Bucket": "art", "Key": "res/manifest.json"}), s3=s3) == ["a", None, "c", "d"]


def test_resolve_rejects_unknown_wrapper():
    with pytest.raises(ValueError):
        resolve(W(zzz=1))


# ---------------------------------------------------------------------------------------------- wrap


def test_wrap_inline_with_facts():
    out = wrap(
        {"score": 0.9, "a": [1, 2]},
        execution="e",
        node="n",
        bucket=None,
        facts={"k0": ["score"], "k1": ["a", 1], "k2": ["nope"]},
    )
    assert out == W(v={"score": 0.9, "a": [1, 2]}, f={"k0": 0.9, "k1": 2, "k2": None})


def test_wrap_offloads_over_threshold(s3):
    out = wrap(
        {"blob": "x" * 500, "tier": "gold"},
        execution="e",
        node="n7",
        bucket="art",
        threshold=100,
        facts={"t": ["tier"]},
        s3=s3,
    )
    assert set(out) == {P.MARK, P.S3, P.FACTS}
    bucket, key = parse_uri(out[P.S3])
    assert bucket == "art" and key.startswith("runs/e/n7/") and key.endswith(".json")
    assert out[P.FACTS] == {"t": "gold"}  # facts are computed even when the value is offloaded
    assert resolve(out, s3=s3)["tier"] == "gold"


def test_wrap_rejects_non_json():
    with pytest.raises(TypeError):
        wrap({1, 2}, execution="e", node="n", bucket=None)


def test_wrap_needs_bucket_to_offload():
    with pytest.raises(RuntimeError):
        wrap("x" * 200, execution="e", node="n", bucket=None, threshold=10)


def test_items_inline_offloads_elements_individually(s3):
    value = {"urls": [{"id": 0, "pad": "s"}, {"id": 1, "pad": "L" * 300}]}
    out = wrap(
        value,
        execution="e",
        node="n",
        bucket="art",
        threshold=100,
        s3=s3,
        items={"k_it": {"path": ["urls"], "mode": "inline", "facts": {"kid": ["id"]}}},
    )
    elems = out[P.ITEMS]["k_it"]
    assert elems[0] == W(v={"id": 0, "pad": "s"}, f={"kid": 0})
    assert P.S3 in elems[1] and elems[1][P.FACTS] == {"kid": 1}
    assert resolve(elems, s3=s3) == value["urls"]
    assert P.S3 in out  # the whole value was over the threshold too


def test_items_s3_facet_writes_array_file(s3):
    value = {"urls": [1, 2, 3]}
    out = wrap(
        value, execution="e", node="n", bucket="art", s3=s3, items={"k": {"path": ["urls"], "mode": "s3"}}
    )
    loc = out[P.ITEMS]["k"]
    body = json.loads(s3.get_object(Bucket=loc["bucket"], Key=loc["key"])["Body"].read())
    assert body == [W(v=1), W(v=2), W(v=3)]


def test_items_must_be_a_list():
    with pytest.raises(TypeError):
        wrap({"urls": 5}, execution="e", node="n", bucket=None, items={"k": {"path": ["urls"]}})


def test_inline_items_too_large_suggests_distributed(s3):
    value = list(range(60_000))
    with pytest.raises(ValueError, match="distributed"):
        wrap(value, execution="e", node="n", bucket="art", s3=s3, items={"k": {"path": [], "mode": "inline"}})


# ------------------------------------------------------------------------------------------- execute


def test_execute_resolves_args_and_wraps(s3):
    out = execute(env("add", {"a": W(v=2), "b": W(pj=W(v={"n": 40}), p=["n"])}), s3=s3, bucket="art")
    assert out == W(v=42)


def test_execute_facts_and_items(s3):
    out = execute(
        env(
            "listing",
            {"n": 3},
            facts={"c": ["count"]},
            items={"u": {"path": ["urls"], "mode": "inline", "facts": {"i": ["id"]}}},
        ),
        s3=s3,
        bucket="art",
    )
    assert out[P.FACTS] == {"c": 3}
    assert [e[P.FACTS]["i"] for e in out[P.ITEMS]["u"]] == [0, 1, 2]


def test_execute_offload_threshold_from_env(s3, monkeypatch):
    monkeypatch.setenv(P.ENV_THRESHOLD, "50")
    monkeypatch.setenv(P.ENV_BUCKET, "art")
    out = execute(env("big", {"n": 100}), s3=s3)
    assert P.S3 in out


def test_execute_errors_propagate_with_their_class():
    with pytest.raises(infra_dummy_steps.PaymentDeclined):
        execute(env("decline", {"order": "o1"}))


def test_execute_rejects_unknown_version():
    with pytest.raises(ValueError):
        execute({"sw": 99, "step": "infra_dummy_steps.add", "args": {}})


def test_execute_imports_workflow_module(monkeypatch):
    import sys

    assert "infra_dummy_late" not in sys.modules
    monkeypatch.setenv(P.ENV_MODULE, "infra_dummy_late")
    envelope = {"sw": 1, "step": "infra_dummy_late.triple", "execution": "e", "node": "n", "args": {"x": 2}}
    assert execute(envelope) == W(v=6)


def test_lambda_handler():
    assert handler(env("add", {"a": 1}), None) == W(v=2)


def test_fact_operators_avoid_copying_values():
    from sfnkit.runtime.results import _facts

    value = {"items": [1, 2, 3], "none": None, "name": "x"}
    got = _facts(value, {"a": ["items", "$len"], "b": ["none", "$null"], "c": ["missing", "$null"],
                         "d": ["name", "$bool"], "e": ["$null"], "f": ["name"]})
    assert got == {"a": 3, "b": True, "c": True, "d": True, "e": False, "f": "x"}
