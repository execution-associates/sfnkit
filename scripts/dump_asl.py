"""Write every state machine of the given workflows to .sfnkit/asl/, placeholders filled with dummies.

Usage: uv run python scripts/dump_asl.py module:attr [module:attr ...]
"""
import importlib
import json
import re
import sys
from pathlib import Path

from sfnkit.compile.app import build_app

out = Path(".sfnkit/asl")
out.mkdir(parents=True, exist_ok=True)
for f in out.glob("*.json"):
    f.unlink()


def fill(obj):
    if isinstance(obj, str):
        def rep(m):
            name = m.group(1)
            if name == "ArtifactsBucket":
                return "sfnkit-dummy-bucket"
            if name.startswith("Sm"):
                return f"arn:aws:states:us-west-2:123456789012:stateMachine:dummy-{name}"
            return f"arn:aws:lambda:us-west-2:123456789012:function:dummy-{name}"
        return re.sub(r"\$\{([A-Za-z0-9]+)\}", rep, obj)
    if isinstance(obj, dict):
        return {k: fill(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [fill(v) for v in obj]
    return obj


for target in sys.argv[1:]:
    mod, attr = target.split(":")
    wf = getattr(importlib.import_module(mod), attr)
    app = build_app(wf)
    for sm in app.state_machines:
        p = out / f"{wf.name}__{sm.name}.json"
        p.write_text(json.dumps(fill(sm.definition), indent=1))
        print(p)
