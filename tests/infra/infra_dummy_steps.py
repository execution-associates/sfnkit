"""Steps used by the infrastructure tests (module-level so the registry can find them)."""

from sfnkit.step import step


class PaymentDeclined(Exception):
    pass


@step
def add(a: int, b: int = 1) -> int:
    return a + b


@step
def echo(value):
    return value


@step
def big(n: int) -> dict:
    return {"blob": "x" * n, "score": 0.9, "tier": "gold"}


@step
def listing(n: int, size: int = 1) -> dict:
    return {"urls": [{"id": i, "pad": "y" * size} for i in range(n)], "count": n}


@step
def decline(order):
    raise PaymentDeclined(f"card declined for {order}")
