"""Check every golden definition with Step Functions' own validator. Free; creates nothing.

Run with AWS credentials in the environment: ``uv run pytest -m aws``.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.aws

GOLDEN = Path(__file__).parent / "core" / "golden"


def _fill(obj):
    """Put plausible ARNs where DefinitionSubstitutions placeholders are."""
    if isinstance(obj, str):
        def rep(m):
            name = m.group(1)
            if name == "ArtifactsBucket":
                return "sfnkit-validate-bucket"
            if name.startswith("Sm"):
                return f"arn:aws:states:us-west-2:123456789012:stateMachine:validate-{name}"
            return f"arn:aws:lambda:us-west-2:123456789012:function:validate-{name}"

        return re.sub(r"\$\{([A-Za-z0-9]+)\}", rep, obj)
    if isinstance(obj, dict):
        return {k: _fill(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_fill(v) for v in obj]
    return obj


def _cases():
    for path in sorted(GOLDEN.glob("*.json")):
        for sm, definition in json.loads(path.read_text()).items():
            yield pytest.param(definition, id=f"{path.stem}/{sm}")


@pytest.mark.parametrize("definition", list(_cases()))
def test_definition_is_valid(definition):
    import boto3

    sfn = boto3.client("stepfunctions", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"))
    res = sfn.validate_state_machine_definition(definition=json.dumps(_fill(definition)), type="STANDARD")
    assert res["result"] == "OK", res.get("diagnostics")
