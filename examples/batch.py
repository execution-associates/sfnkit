"""Steps on AWS Batch (Fargate Spot): single jobs, array jobs and errors by name. The Batch e2e test
deploys it.

SFNKIT_SUBNETS and SFNKIT_SECURITY_GROUPS (comma-separated ids) choose the network at deploy time. The
subnets need a route to ECR, S3 and Step Functions; public subnets work, since tasks get a public IP.
"""

from __future__ import annotations

import os

from sfnkit import Batch, Err, FargateQueue, Ok, attempt, length, step, workflow


def _ids(name: str, placeholder: str) -> tuple[str, ...]:
    # The deployed runtime imports this module too, without these variables; the queue is only read at synth.
    return tuple(x for x in os.environ.get(name, "").split(",") if x) or (placeholder,)


queue = FargateQueue(
    subnets=_ids("SFNKIT_SUBNETS", "subnet-00000000"),
    security_groups=_ids("SFNKIT_SECURITY_GROUPS", "sg-00000000"),
    spot=True,
    max_vcpus=32,
)
tiny = Batch(queue=queue, vcpus=0.25, memory=512, timeout=600)


@step
def make_items(n: int) -> list[dict]:
    return [{"i": i} for i in range(n)]


@step(compute=tiny)
def square(item: dict, fail_at: int = -1) -> dict:
    if item["i"] == fail_at:
        raise LookupError(f"item {fail_at} failed on purpose")
    return {"i": item["i"], "sq": item["i"] ** 2, "job": os.environ.get("AWS_BATCH_JOB_ID", "local")}


@step(compute=tiny)
def fragile(item: dict, bad: list[int] | None = None) -> int:
    if item["i"] in (bad or []):
        raise ValueError(f"item {item['i']} is bad")
    return item["i"]


@step(compute=Batch(queue=queue, vcpus=0.25, memory=512, timeout=600, items_per_job=10))
def plus_one(r: dict) -> int:
    return r["sq"] + 1


@step(compute=tiny)
def total(values: list[int]) -> int:
    return sum(values)


@step(compute=tiny)
def picky(x: int) -> int:
    if x < 0:
        raise ValueError(f"{x} is negative")
    return x * 10


@step
def summarize(squares: list[dict], plus: list[int], extra: list[dict], t: int, lenient: list) -> dict:
    return {
        "n": len(squares),
        "ordered": [s["i"] for s in squares] == list(range(len(squares))),
        "jobs": len({s["job"] for s in squares}),
        "plus_ok": plus == [s["sq"] + 1 for s in squares],
        "extra": [e["sq"] for e in extra],
        "total": t,
        "skipped": [i for i, v in enumerate(lenient) if v is None],  # tolerated failures read as None
    }


@workflow(exclude=["tests", "src", "scripts", "docs"])
def batchy(n: int = 100, x: int = 1, fail_at: int = -1, bad: list | None = None):
    items = make_items(n)
    squares = square.map(items, fail_at=fail_at)  # one array job: n children
    plus = plus_one.map(squares)  # over the previous array's results, 10 items per child
    extra = square.map([{"i": 7}, {"i": 8}])  # a literal list is written to S3 first
    t = total(plus)  # one Batch job reading array results
    lenient = fragile.map(items, bad=bad, tolerated_failure_count=1)  # one bad item is recorded, not fatal

    match attempt(picky, x):  # a Batch step's error, caught by its Python class name
        case Ok(v):
            checked = v
        case Err("ValueError", cause):
            checked = "value-error"

    if length(squares) > 50:
        size = "big"
    else:
        size = "small"
    return {"summary": summarize(squares, plus, extra, t, lenient), "checked": checked, "size": size}
