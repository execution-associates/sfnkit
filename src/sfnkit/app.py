"""The compiled form of a workflow: everything CloudFormation assembly and the deployer need.

``sfnkit.compile.build_app(workflow)`` produces an ``App``. Downstream code (``compile/cfn.py``,
``build/``, ``deploy/``) reads only these types, never the tracer or IR.

Conventions shared with CloudFormation assembly:
  * State machine definitions use ``DefinitionSubstitutions`` placeholders. ``StateMachineSpec
    .substitutions`` already maps every placeholder it uses to a CFN intrinsic, except that the
    intrinsics may reference the template parameter ``ArtifactsBucket`` (the assets stack's bucket name)
    and logical ids of functions in ``App.functions``.
  * A state machine's full name is ``${AWS::StackName}-<StateMachineSpec.name>`` so ARNs can be built
    without a CloudFormation dependency cycle (``sm_arn``).
  * Each ``ImageVariant`` becomes a template parameter ``ImageVariant.param_name`` holding the image URI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from sfnkit.compute.base import ComputeSpec
from sfnkit.iam import Policy

if TYPE_CHECKING:
    from sfnkit.step import Step
    from sfnkit.workflow import Workflow


@dataclass(eq=False)
class ImageVariant:
    key: str  # short content hash of the dependency set
    deps: tuple[str, ...]
    system_packages: tuple[str, ...]
    python: str  # e.g. "3.14"
    architecture: str  # "x86_64" | "arm64"
    runtime: str = "lambda"  # "lambda" | "container": what the image starts (ComputeBackend.image_runtime)
    base: str | None = None  # a custom base image (sfnkit.Image); None is the AWS Lambda Python base
    conda: tuple[str, ...] = ()
    channels: tuple[str, ...] = ()
    python_mode: str | None = None  # Image.python: None (auto), "base" or a version

    @property
    def param_name(self) -> str:
        return f"Image{self.key}Uri"

    @property
    def custom(self) -> bool:
        return self.base is not None


@dataclass(eq=False)
class FunctionSpec:
    """One deployed unit of compute: a step with its resolved compute, IAM policy and image."""

    logical_id: str
    step: Step
    compute: ComputeSpec
    policy: Policy  # the user's effective policy (base set or the step's own); baseline added at synth
    image: ImageVariant
    env: dict[str, str] = field(default_factory=dict)


@dataclass(eq=False)
class SmNeed:
    """Something a state machine role must be allowed to do. CFN assembly turns these into statements."""

    kind: Literal["invoke", "start_child", "map_run", "bucket", "s3_read"]
    fn: FunctionSpec | None = None  # invoke
    child: str | None = None  # start_child: StateMachineSpec.name of the child
    bucket: str | None = None  # s3_read: a user bucket name
    prefix: str | None = None  # s3_read: key prefix ("" for the whole bucket)


@dataclass(eq=False)
class StateMachineSpec:
    logical_id: str
    name: str  # suffix of the full name (see module docstring)
    definition: dict[str, Any]
    substitutions: dict[str, Any]
    type: Literal["STANDARD", "EXPRESS"]
    needs: list[SmNeed]
    root: bool = False


@dataclass(eq=False)
class AppSettings:
    offload_threshold: int = 32 * 1024
    log_retention_days: int = 14
    run_retention_days: int = 30
    python: str = "3.14"
    architecture: str = "x86_64"


@dataclass(eq=False)
class App:
    name: str  # sanitized root workflow name; the default stack name is f"sfnkit-{name}"
    module: str  # importable module that defines the root workflow (and registers its steps)
    workflow: Workflow
    functions: list[FunctionSpec]
    state_machines: list[StateMachineSpec]  # root first
    images: list[ImageVariant]
    settings: AppSettings
    project_dir: Path
    exclude: tuple[str, ...] = ()

    @property
    def root(self) -> StateMachineSpec:
        return self.state_machines[0]


def sm_full_name(name: str) -> dict:
    return {"Fn::Sub": f"${{AWS::StackName}}-{name}"}


def sm_arn(name: str) -> dict:
    return {
        "Fn::Sub": "arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:stateMachine:"
        f"${{AWS::StackName}}-{name}"
    }


def sm_execution_arn(name: str) -> dict:
    return {
        "Fn::Sub": "arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:execution:"
        f"${{AWS::StackName}}-{name}:*"
    }
