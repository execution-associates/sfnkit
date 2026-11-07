# examples/checkout.py
import json

import boto3

from sfnkit import Allow, Err, Lambda, Ok, Policy, Retry, attempt, fail, step, subworkflow, wait, workflow

BUCKET = "acme-orders"  # point this at your own bucket

# The workflow's base IAM set. A step without its own iam= gets exactly this.
base = Policy(Allow("s3:GetObject", f"arn:aws:s3:::{BUCKET}/orders/*"))


class PaymentDeclined(Exception):
    pass


@step
def load_order(key: str) -> dict:
    body = boto3.client("s3").get_object(Bucket=BUCKET, Key=key)["Body"]
    return json.load(body)  # {"id": "o-17", "card": "4242...", "items": [{"sku": "mug", "qty": 2}]}


@step(
    deps=["pyyaml>=6"],  # added to this step's image only
    iam=Policy(Allow("s3:GetObject", f"arn:aws:s3:::{BUCKET}/prices.yaml")),  # this, and none of base
)
def load_prices() -> dict:
    import yaml

    body = boto3.client("s3").get_object(Bucket=BUCKET, Key="prices.yaml")["Body"]
    return yaml.safe_load(body)  # {"mug": 12.5, "lamp": 300}


@step
def price_item(item: dict, prices: dict) -> float:
    return prices[item["sku"]] * item["qty"]


@step
def order_total(line_totals: list[float]) -> float:
    return round(sum(line_totals), 2)


@step
def score_risk(total: float) -> str:
    return "low" if total < 100 else "medium" if total < 1000 else "high"  # stand-in for a fraud model


@step(compute=Lambda(timeout=120), retry=Retry(on=[TimeoutError], max_attempts=3))
def charge(card: str, amount: float) -> str:
    if card.startswith("4000"):  # stand-in for a payment provider
        raise PaymentDeclined(f"card ending {card[-4:]} declined")
    return f"rcpt-{card[-4:]}-{amount}"


@step(iam=base + Allow("s3:PutObject", f"arn:aws:s3:::{BUCKET}/declined/*"))  # base, plus this
def record_decline(order: dict, cause: dict) -> None:
    body = json.dumps({"order": order["id"], "error": cause["errorMessage"]})
    boto3.client("s3").put_object(Bucket=BUCKET, Key=f"declined/{order['id']}.json", Body=body)


@workflow(iam=base)
def checkout(key: str):
    order = load_order(key)  # these two don't depend on each other,
    prices = load_prices()  # so they run side by side in a Parallel state
    line_totals = price_item.map(order["items"], prices=prices, max_concurrency=10)  # Map state

    with subworkflow("risk"):  # a child state machine
        total = order_total(line_totals)  # sum() on a step's output has to happen in a step
        risk = score_risk(total)

    match risk:  # Choice state
        case "low":
            pass
        case "medium" if total < 500:
            wait(seconds=300)  # Wait state: hold the order for 5 minutes
        case _:
            fail("HighRisk", "order needs manual review")  # Fail state

    match attempt(charge, order["card"], total):  # Catch
        case Ok(receipt):
            pass
        case Err("PaymentDeclined", cause):  # any other error fails the workflow
            record_decline(order, cause)
            receipt = None

    return {"order": order["id"], "total": total, "receipt": receipt}
