"""sfnkit: author AWS Step Functions workflows as decorated Python functions."""

from sfnkit.blocks import (
    Err,
    Ok,
    S3File,
    S3Objects,
    States,
    attempt,
    each,
    fail,
    is_null,
    length,
    one_of,
    subworkflow,
    wait,
)
from sfnkit.compute import ComputeBackend, ComputeSpec, Lambda, register_backend
from sfnkit.errors import CompileError, DeployError, LintError, SfnkitError, TraceError
from sfnkit.iam import Allow, Deny, Policy
from sfnkit.local import WorkflowFailed
from sfnkit.step import Retry, Step, step
from sfnkit.workflow import Workflow, workflow

__version__ = "0.1.0"

__all__ = [
    "Allow",
    "CompileError",
    "ComputeBackend",
    "ComputeSpec",
    "Deny",
    "DeployError",
    "Err",
    "Lambda",
    "LintError",
    "Ok",
    "Policy",
    "Retry",
    "S3File",
    "S3Objects",
    "States",
    "Step",
    "SfnkitError",
    "TraceError",
    "Workflow",
    "WorkflowFailed",
    "attempt",
    "each",
    "fail",
    "is_null",
    "length",
    "one_of",
    "register_backend",
    "step",
    "subworkflow",
    "wait",
    "workflow",
]


def deploy(workflow, **kwargs):
    """Build, push and deploy a workflow. See ``sfnkit.deploy.deployer.deploy``."""
    from sfnkit.compile.app import build_app
    from sfnkit.deploy.deployer import deploy as _deploy

    return _deploy(build_app(workflow), **kwargs)
