"""The README's example is examples/checkout.py verbatim, and every path through it runs."""

import json
import re
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from sfnkit.local import WorkflowFailed

ROOT = Path(__file__).resolve().parent.parent


def test_readme_shows_examples_checkout():
    readme = (ROOT / "README.md").read_text()
    block = re.search(r"```python\n(.*?)```", readme, re.S).group(1)
    assert block == (ROOT / "examples" / "checkout.py").read_text()


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket="acme-orders")
        client.put_object(Bucket="acme-orders", Key="prices.yaml", Body="mug: 12.5\nlamp: 300\n")
        yield client


def put_order(s3, oid: str, card: str, items: list) -> str:
    key = f"orders/{oid}.json"
    s3.put_object(Bucket="acme-orders", Key=key, Body=json.dumps({"id": oid, "card": card, "items": items}))
    return key


def test_low_risk_is_charged(s3):
    from examples.checkout import checkout

    key = put_order(s3, "o-1", "4242424242424242", [{"sku": "mug", "qty": 2}])
    assert checkout.local(key) == {"order": "o-1", "total": 25.0, "receipt": "rcpt-4242-25.0"}


def test_medium_risk_waits_then_charges(s3):
    from examples.checkout import checkout

    key = put_order(s3, "o-2", "4242424242424242", [{"sku": "lamp", "qty": 1}, {"sku": "mug", "qty": 4}])
    assert checkout.local(key)["receipt"] == "rcpt-4242-350.0"


def test_high_risk_fails(s3):
    from examples.checkout import checkout

    key = put_order(s3, "o-3", "4242424242424242", [{"sku": "lamp", "qty": 2}])
    with pytest.raises(WorkflowFailed) as e:
        checkout.local(key)
    assert e.value.error == "HighRisk"


def test_declined_card_is_recorded(s3):
    from examples.checkout import checkout

    key = put_order(s3, "o-4", "4000000000000002", [{"sku": "mug", "qty": 1}])
    assert checkout.local(key) == {"order": "o-4", "total": 12.5, "receipt": None}
    body = json.load(s3.get_object(Bucket="acme-orders", Key="declined/o-4.json")["Body"])
    assert body == {"order": "o-4", "error": "card ending 0002 declined"}


def test_what_the_comments_claim():
    from examples import checkout as ex
    from sfnkit import Allow, Policy
    from sfnkit.compile.app import build_app

    app = build_app(ex.checkout)
    fns = {fn.step.name: fn for fn in app.functions}
    assert set(fns) == {
        "load_order",
        "load_prices",
        "price_item",
        "order_total",
        "score_risk",
        "charge",
        "record_decline",
    }  # every function the body calls is its own Lambda function
    assert fns["load_order"].policy == ex.base
    assert fns["load_prices"].policy == Policy(Allow("s3:GetObject", "arn:aws:s3:::acme-orders/prices.yaml"))
    assert fns["record_decline"].policy == ex.base + Allow(
        "s3:PutObject", "arn:aws:s3:::acme-orders/declined/*"
    )
    assert "pyyaml>=6" in fns["load_prices"].image.deps
    assert "pyyaml>=6" not in fns["load_order"].image.deps
    assert fns["charge"].compute.timeout == 120
    assert len(app.state_machines) == 2  # checkout, plus the "risk" child

    states = json.dumps(ex.checkout.to_asl())
    for kind in ("Parallel", "Map", "Choice", "Wait", "Fail"):
        assert f'"Type": "{kind}"' in states


def test_golden():
    """Snapshot of the compiled ASL, which tests/test_aws_validate.py also checks against AWS."""
    import os

    from examples.checkout import checkout
    from sfnkit.compile.app import build_app

    got = {sm.name: sm.definition for sm in build_app(checkout).state_machines}
    path = ROOT / "tests" / "core" / "golden" / "checkout.json"
    if os.environ.get("SFNKIT_UPDATE_GOLDEN") or not path.exists():
        path.write_text(json.dumps(got, indent=1, sort_keys=True) + "\n")
    assert json.loads(path.read_text()) == json.loads(json.dumps(got))


def _block(containing: str) -> str:
    readme = (ROOT / "README.md").read_text()
    return next(b for b in re.findall(r"```python\n(.*?)```", readme, re.S) if containing in b)


def _run_block(code: str, name: str) -> dict:
    """Run a README block as if it were a module, so its steps pass the module-level check."""
    import sys
    import types

    mod = types.ModuleType(name)
    sys.modules[name] = mod
    try:
        exec(compile(code, f"README:{name}", "exec"), mod.__dict__)
    finally:
        sys.modules.pop(name, None)
    return mod.__dict__


def test_readme_image_example_defines_everything_it_uses():
    ns = _run_block(_block("cuda = Image("), "readme_images")
    assert ns["call_variants"].image.conda == ("samtools=1.21", "bcftools")
    assert ns["train"].compute.queue.name == "gpu"


def test_readme_cdk_example_synthesizes(tmp_path, monkeypatch):
    import functools
    import os

    os.environ.setdefault("JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION", "1")
    cdk = pytest.importorskip("aws_cdk")
    # CDK_OUTDIR wouldn't reach CDK's node process once it's running, so pin the outdir on App itself.
    monkeypatch.setattr(cdk, "App", functools.partial(cdk.App, outdir=str(tmp_path / "cdk.out")))
    monkeypatch.chdir(ROOT)
    _run_block(_block("SfnkitWorkflow(self"), "readme_cdk")
    template = json.loads((tmp_path / "cdk.out" / "cohort.template.json").read_text())
    kinds = [r["Type"] for r in template["Resources"].values()]
    assert kinds.count("AWS::StepFunctions::StateMachine") == 2 and "AWS::Scheduler::Schedule" in kinds
