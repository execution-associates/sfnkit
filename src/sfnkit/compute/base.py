"""The compute backend contract: the one extension point for running steps somewhere new.

A backend turns one ``FunctionSpec`` (a step with its resolved compute, IAM policy and image) into:
  * CloudFormation resources that run it (``resources``),
  * IAM statements its own role needs beyond the user's policy (``baseline_statements``),
  * the Step Functions Task state that invokes it (``task_state``),
  * IAM statements the state machine role needs to invoke it (``state_machine_statements``).

Nothing in the compiler, CloudFormation assembly or deployer imports a concrete backend; they only call
``backend_for(spec)``. Third-party backends register through the ``sfnkit.compute`` entry point.

Runtime side: every backend must deliver the envelope from ``sfnkit.runtime.protocol`` to
``sfnkit.runtime.execute.execute`` and hand its returned wrapper back to Step Functions. Lambda does
this with the event and return value; a container backend would use a task token (envelope via env/S3,
result via SendTaskSuccess).
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


# Placeholder the backend puts where the envelope goes in its Task ``Arguments``.
PAYLOAD = object()


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
