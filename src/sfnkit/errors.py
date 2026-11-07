class SfnkitError(Exception):
    """Base class for every error sfnkit raises on purpose."""


class TraceError(SfnkitError):
    """The workflow body can't be turned into a graph (raised while tracing)."""


class LintError(TraceError):
    """The workflow body uses Python syntax that tracing would get wrong."""


class CompileError(SfnkitError):
    """The graph can't be expressed as a state machine."""


class DeployError(SfnkitError):
    """Building, pushing or deploying failed."""


class ToleratedFailureExceeded(SfnkitError):
    """More items of a map failed than ``tolerated_failure_count``/``_percentage`` allows. Batch array
    jobs and local runs raise it; a deployed Distributed Map reports
    ``States.ExceedToleratedFailureThreshold`` instead."""
