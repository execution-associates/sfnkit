"""Workflows used by the core tests. Steps must be module-level, so they live here."""

from sfnkit import (
    Allow,
    Err,
    Lambda,
    Ok,
    Policy,
    Retry,
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


class PaymentDeclined(Exception):
    pass


class errors:  # stands in for an `errors` module so dotted names work in patterns
    PaymentDeclined = PaymentDeclined


base = Policy(Allow("s3:GetObject", "arn:aws:s3:::data/*"))


@step(compute=Lambda(memory=1024, timeout=120), deps=["httpx"])
def fetch(url: str) -> list[str]:
    return [f"{url}/a", f"{url}/b", f"{url}/c"]


@step(deps=["pyyaml"], retry=Retry(on=["ValueError"], max_attempts=2), iam=base + Allow("dynamodb:PutItem", "*"))
def parse(doc: str) -> dict:
    return {"doc": doc, "len": len(doc)}


@step(iam=Policy(Allow("sqs:SendMessage", "*")))
def notify(msg: dict) -> dict:
    return {"sent": msg}


@step
def score_rows(rows: list) -> dict:
    total = sum(r["len"] for r in rows)
    tier = "gold" if total > 60 else "review" if total > 30 else "low"
    return {"tier": tier, "value": total / 100}


@step
def audit_rows(rows: list) -> dict:
    return {"count": len(rows)}


@step
def publish(rows: list) -> dict:
    return {"order": "published", "n": len(rows)}


@step
def queue_for_review(rows: list) -> dict:
    return {"order": "queued", "n": len(rows)}


@step
def charge(order: dict) -> dict:
    if order.get("n", 0) == 2:
        raise PaymentDeclined("card declined")
    if order.get("n", 0) == 1:
        raise TimeoutError("gateway slow")
    if order.get("n", 0) == 5:
        raise KeyError("boom")
    return {"receipt": f"r-{order['order']}"}


@step
def refund(order: dict, cause: dict) -> dict:
    return {"refunded": order["order"], "why": cause.get("errorType") if isinstance(cause, dict) else cause}


@step
def enrich(row: dict, label: str) -> dict:
    return {**row, "label": label}


@step
def double(x: int) -> int:
    return x * 2


@step
def add(a: int, b: int) -> int:
    return a + b


@workflow
def simple(x: int):
    y = double(x)
    return add(y, x)


@workflow
def branchy(x: int):
    y = double(x)
    if y > 10:
        z = add(y, 1)
    elif y > 4:
        z = add(y, 2)
    else:
        z = add(y, 3)
    return {"z": z, "y": y}


@workflow
def matcher(tier: str, value: float):
    match tier:
        case "gold" | "silver":
            r = double(1)
        case "review" if value > 0.5:
            r = double(2)
        case _:
            fail("LowScore", "score below threshold")
    return r


@workflow
def catcher(n: int):
    order = add(n, 0)
    match attempt(charge, {"order": "o", "n": order}):
        case Ok(receipt):
            pass
        case Err(errors.PaymentDeclined | "TimeoutError", cause):
            receipt = refund({"order": "o"}, cause)
        case Err(error, cause):
            receipt = None
    if is_null(receipt):
        notify({"msg": "no receipt"})
    return receipt


@workflow
def narrow_catch(n: int):
    match attempt(charge, {"order": "o", "n": n}):
        case Ok(receipt):
            pass
        case Err("PaymentDeclined", cause):
            receipt = {"declined": True}
    return receipt


@step
def charge_and_notify(order: dict) -> dict:
    return charge.fn(order)


def charge_twice(order):
    first = charge(order)
    return notify(first)


@workflow
def block_attempt(n: int):
    match attempt(charge_twice, {"order": "o", "n": n}):
        case Ok(v):
            out = v
        case Err(e, c):
            out = {"error": e}
    return out


@workflow
def mapper(url: str, label: str = "run"):
    docs = fetch(url)
    rows = parse.map(docs, max_concurrency=2)
    with each(docs, distributed=True) as it:
        p = parse(it.item)
        if p["len"] > 100:
            q = enrich(p, "long")
        else:
            q = enrich(p, label)
        it.result(q)
    enriched = it.value
    with subworkflow("scoring"):
        score = score_rows(rows)
        audit = audit_rows(enriched)
    if length(rows) > 0:
        wait(seconds=1)
    return {"score": score, "audit": audit, "enriched": enriched}


@workflow(iam=base)
def pipeline(url: str, label: str = "run"):
    docs = fetch(url)
    rows = parse.map(docs, max_concurrency=10)
    with each(docs, distributed=True, max_concurrency=50) as it:
        it.result(enrich(parse(it.item), label))
    enriched = it.value
    with subworkflow("scoring"):
        score = score_rows(enriched)
        audit = audit_rows(enriched)
    match score["tier"]:
        case "gold" | "silver":
            order = publish(rows)
        case "review" if score["value"] > 0.5:
            wait(seconds=2)
            order = queue_for_review(rows)
        case _:
            fail("LowScore", "score below 0.5")
    match attempt(charge, order):
        case Ok(receipt):
            pass
        case Err(errors.PaymentDeclined | "TimeoutError", cause):
            receipt = refund(order, cause)
        case Err(error, cause):
            notify({"error": error, "cause": cause})
            receipt = None
    if is_null(receipt):
        notify({"msg": "no receipt"})
    return {"receipt": receipt, "audit": audit}


@workflow
def child_wf(x: int):
    return double(x)


@workflow
def parent_wf(x: int):
    a = child_wf(x)
    b = child_wf.map([1, 2, 3])
    return {"a": a, "b": b}
