"""An example that exercises every construct; the end-to-end test deploys and runs it.

Inputs pick the path: ``tier`` drives the match, ``charge_mode`` the attempt, ``strict_mode`` a second
attempt whose unhandled errors fail the workflow, ``n`` the number of documents.
"""

from __future__ import annotations

import os

from sfnkit import (
    Allow,
    Err,
    Ok,
    Policy,
    attempt,
    each,
    fail,
    is_null,
    length,
    step,
    subworkflow,
    wait,
    workflow,
)

# The artifacts bucket's name is only known at deploy time, so the policy uses CloudFormation's
# ${ArtifactsBucket} parameter. fixtures/ is outside the runtime baseline (which covers runs/ only).
FIXTURES = {"Fn::Sub": "arn:${AWS::Partition}:s3:::${ArtifactsBucket}/fixtures/*"}
BUCKET = {"Fn::Sub": "arn:${AWS::Partition}:s3:::${ArtifactsBucket}"}

base = Policy(Allow("s3:GetObject", FIXTURES))


class PaymentDeclined(Exception):
    pass


class errors:
    PaymentDeclined = PaymentDeclined


# steps with their own dependencies (two image variants)


@step(deps=["pyyaml>=6"])
def make_docs(n: int) -> list[dict]:
    import yaml

    return [yaml.safe_load(f"id: {i}\ntext: doc number {i}") for i in range(n)]


@step(deps=["humanize>=4"])
def describe(doc: dict) -> dict:
    import humanize

    return {**doc, "words": humanize.apnumber(len(doc["text"].split()))}


@step
def tag(doc: dict, label: str) -> dict:
    return {**doc, "label": label}


@step
def big_output(n: int) -> list[str]:
    # ~100 KB, over the 32 KiB inline threshold, so it travels through S3
    return [f"{i:06d}-" + "x" * 44 for i in range(max(n, 1) * 40)]


@step
def measure(blob: list) -> dict:
    return {"items": len(blob), "bytes": sum(len(s) for s in blob)}


# IAM: inherit, extend, or replace the workflow's base set


def _read_fixture() -> dict:
    import boto3

    body = boto3.client("s3").get_object(Bucket=os.environ["SFNKIT_BUCKET"], Key="fixtures/data.json")["Body"]
    return {"read": body.read().decode()}


@step
def read_fixture() -> dict:
    return _read_fixture()


@step(iam=base + Allow("s3:ListBucket", BUCKET))
def read_fixture_plus() -> dict:
    import boto3

    out = _read_fixture()
    listing = boto3.client("s3").list_objects_v2(Bucket=os.environ["SFNKIT_BUCKET"], Prefix="fixtures/")
    out["listed"] = [o["Key"] for o in listing.get("Contents", [])]
    return out


@step(iam=Policy(Allow("sqs:ListQueues", "*")))
def read_fixture_own() -> dict:
    from botocore.exceptions import ClientError

    try:
        return _read_fixture()
    except ClientError as e:
        return {"denied": e.response["Error"]["Code"]}


# scoring, ordering, charging


@step
def score_rows(rows: list, tier: str, value: float) -> dict:
    return {"tier": tier, "value": value, "rows": len(rows)}


@step
def publish(score: dict) -> dict:
    return {"order": "published", "tier": score["tier"]}


@step
def queue_for_review(score: dict) -> dict:
    return {"order": "queued", "tier": score["tier"]}


@step
def charge(order: dict, mode: str) -> dict:
    if mode == "declined":
        raise PaymentDeclined("card declined")
    if mode == "boom":
        raise KeyError("unexpected")
    return {"receipt": f"r-{order['order']}"}


@step
def refund(order: dict, cause: dict) -> dict:
    return {"refunded": order["order"], "error_type": cause.get("errorType")}


@step
def notify(msg: dict) -> dict:
    return {"notified": msg}


@step
def strict(mode: str) -> str:
    if mode == "value":
        raise ValueError("bad value")
    if mode == "strict":
        raise LookupError("not handled by the workflow")
    return "fine"


# a reusable child workflow (its own state machine)


@step
def count_rows(rows: list) -> int:
    return len(rows)


@workflow
def summarize(rows: list):
    total = count_rows(rows)
    if length(rows) > 3:
        size = "many"
    else:
        size = "few"
    return {"total": total, "size": size}


@workflow(iam=base, exclude=["tests", "src", "scripts", "docs"])
def pipeline(
    n: int = 50,
    tier: str = "gold",
    value: float = 0.9,
    charge_mode: str = "ok",
    strict_mode: str = "ok",
):
    docs = make_docs(n)
    described = describe.map(docs, max_concurrency=10)  # inline Map

    with each(docs, distributed=True, max_concurrency=25) as it:  # Distributed Map, multi-step body
        d = describe(it.item)
        if d["id"] < 25:
            t = tag(d, "first-half")
        else:
            t = tag(d, "second-half")
        it.result(t)
    tagged = it.value

    fixture = read_fixture()  # these three run in one Parallel level
    fixture_plus = read_fixture_plus()
    fixture_own = read_fixture_own()
    blob = big_output(n)

    with subworkflow("scoring"):  # inline child state machine
        score = score_rows(tagged, tier, value)
        size = measure(blob)

    summary = summarize(described)  # reusable child state machine

    match score["tier"]:
        case "gold" | "silver":
            order = publish(score)
        case "review" if score["value"] > 0.5:
            wait(seconds=3)
            order = queue_for_review(score)
        case _:
            fail("LowScore", "score below threshold")

    match attempt(charge, order, charge_mode):
        case Ok(receipt):
            pass
        case Err(errors.PaymentDeclined | "TimeoutError", cause):
            receipt = refund(order, cause)
        case Err(error, cause):
            notify({"error": error})
            receipt = None

    match attempt(strict, strict_mode):
        case Ok(v):
            checked = v
        case Err("ValueError", cause):
            checked = "value-error"

    if is_null(receipt):
        note = notify({"msg": "no receipt"})
    else:
        note = "receipt ok"

    return {
        "receipt": receipt,
        "order": order,
        "note": note,
        "checked": checked,
        "size": size,
        "summary": summary,
        "fixture": fixture,
        "fixture_plus": fixture_plus,
        "fixture_own": fixture_own,
        "tagged": tagged,
        "described": described,
    }
