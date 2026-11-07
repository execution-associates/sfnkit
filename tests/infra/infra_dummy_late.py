"""Imported only through SFNKIT_MODULE, to prove the runtime imports the workflow module."""

from sfnkit.step import step


@step
def triple(x: int) -> int:
    return 3 * x
