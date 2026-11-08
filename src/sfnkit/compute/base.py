"""The compute backend contract: the one extension point for running steps somewhere new.

A backend turns one ``FunctionSpec`` (a step with its resolved compute, IAM policy and image) into:
  * CloudFormation resources that run it (``resources``),
  * IAM statements its own role needs beyond the user's policy (``baseline_statements``),
  * the Step Functions Task state that invokes it (``task_state``),
  * IAM statements the state machine role needs to invoke it (``state_machine_statements``).

Nothing in the compiler, CloudFormation assembly or deployer imports a concrete backend; they only call
``backend_for(spec)``. Third-party backends register through the ``sfnkit.compute`` entry point.

Optional capabilities, as class attributes:
  * ``image_runtime``: ``"lambda"`` builds the image with the Lambda handler as its command;
    ``"container"`` makes ``python -m sfnkit.runtime.container`` the entrypoint.
  * ``standard_only``: Step Functions can only wait for this service in STANDARD workflows, so the
    compiler rejects the step inside an EXPRESS workflow or an EXPRESS Distributed Map child.
  * ``supports_array`` + ``array_task_state``: ``step.map(items)`` becomes one native array job
    instead of a Map state (see ``sfnkit.compute.batch``).
  * ``stage_envelope``: the state machine writes the envelope to the artifacts bucket first and the
    Task gets an S3 wrapper in its place, for services that cap request sizes well below 256 KiB.
  * ``parameters``: template parameters the resources reference (a queue bound at deploy time, say).

Runtime side: every backend must deliver the envelope from ``sfnkit.runtime.protocol`` to
``sfnkit.runtime.execute.execute`` and hand its returned wrapper back to Step Functions. Lambda does
this with the event and return value. Container backends pass the envelope and the task token in the
environment, and ``sfnkit.runtime.container`` reports with SendTaskSuccess/SendTaskFailure.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

if TYPE_CHECKING:
    from sfnkit.app import FunctionSpec, ImageVariant


class ComputeSpec:
    """Base class for user-facing compute settings (``Lambda(...)``, later ``Fargate(...)``)."""

    kind: ClassVar[str] = "abstract"


# Placeholders the backend puts where the envelope goes in its Task ``Arguments``: PAYLOAD as a JSON
# object, PAYLOAD_JSON as a string holding that JSON (for an environment variable).
PAYLOAD = object()
PAYLOAD_JSON = object()


@dataclass
class TaskState:
    resource: str
    arguments: dict[str, Any]  # contains PAYLOAD somewhere
    result: str  # JSONata over $states.result that yields the step's result wrapper
    timeout_seconds: int | None = None
    substitutions: dict[str, Any] = field(default_factory=dict)  # DefinitionSubstitutions entries


class SynthContext(Protocol):
    """What CloudFormation assembly gives a backend while it writes resources."""

    log_retention_days: int

    def image_uri(self, image: ImageVariant) -> Any: ...  # CFN intrinsic

    def role_arn(self, fn: FunctionSpec) -> Any: ...  # CFN intrinsic for the step's role

    def runtime_env(self, fn: FunctionSpec) -> dict[str, Any]: ...  # env vars the runtime needs

    def tags(self) -> list[dict[str, str]]: ...


class ComputeBackend(ABC):
    spec_type: ClassVar[type[ComputeSpec]]
    assume_role_service: ClassVar[str]

    @abstractmethod
    def resources(self, fn: FunctionSpec, ctx: SynthContext) -> dict[str, dict]: ...

    @abstractmethod
    def baseline_statements(self, fn: FunctionSpec, ctx: SynthContext) -> list[dict]: ...

    @abstractmethod
    def task_state(self, fn: FunctionSpec) -> TaskState: ...

    @abstractmethod
    def state_machine_statements(self, fn: FunctionSpec) -> list[dict]: ...

    def default_retry(self) -> list[dict]:
        return []

    image_runtime: ClassVar[str] = "lambda"
    standard_only: ClassVar[bool] = False
    supports_array: ClassVar[bool] = False
    stage_envelope: ClassVar[bool] = False

    def parameters(self, fn: FunctionSpec) -> dict[str, dict]:
        """Template parameters this step's resources reference, bound at deploy time."""
        return {}

    def array_task_state(self, fn: FunctionSpec, size: str) -> TaskState:
        """The Task that runs ``size`` copies of the step as one array job (``size`` is JSONata).
        Only called when ``supports_array`` is true."""
        raise NotImplementedError


_BACKENDS: dict[type, ComputeBackend] = {}
_loaded_entry_points = False


def register_backend(backend: type[ComputeBackend] | ComputeBackend) -> None:
    inst = backend() if isinstance(backend, type) else backend
    _BACKENDS[inst.spec_type] = inst


def _load_entry_points() -> None:
    global _loaded_entry_points
    if _loaded_entry_points:
        return
    _loaded_entry_points = True
    for ep in entry_points(group="sfnkit.compute"):
        try:
            cls = ep.load()
        except Exception:  # a broken plugin must not break every other backend
            continue
        if cls.spec_type not in _BACKENDS:
            register_backend(cls)


def backend_for(spec: ComputeSpec) -> ComputeBackend:
    for cls in type(spec).__mro__:
        if cls in _BACKENDS:
            return _BACKENDS[cls]
    _load_entry_points()
    for cls in type(spec).__mro__:
        if cls in _BACKENDS:
            return _BACKENDS[cls]
    raise LookupError(f"no compute backend registered for {type(spec).__name__}")
