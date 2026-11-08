"""``sfnkit synth|deploy|run|local|destroy module:workflow``."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

from sfnkit.errors import SfnkitError


def load_workflow(target: str, project_dir: Path):
    module, sep, attr = target.partition(":")
    if not sep or not module or not attr:
        raise SystemExit(f"expected module:workflow, got {target!r}")
    root = str(project_dir.resolve())
    if root not in sys.path:
        sys.path.insert(0, root)
    mod = importlib.import_module(module)
    obj = mod
    for part in attr.split("."):
        obj = getattr(obj, part)
    return obj


def parse_input(raw: str | None) -> Any:
    if raw is None:
        return {}
    if raw.startswith("@"):
        raw = Path(raw[1:]).read_text()
    try:
        return json.loads(raw)
    except ValueError as e:
        raise SystemExit(f"--input is not valid JSON: {e}") from None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sfnkit", description="Author AWS Step Functions workflows in Python")
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp, aws: bool):
        sp.add_argument("target", help="module:workflow, e.g. myapp.flows:pipeline")
        sp.add_argument("--project-dir", type=Path, default=Path.cwd(), help="project root (default: cwd)")
        if aws:
            sp.add_argument("--region")
            sp.add_argument("--profile")
            sp.add_argument("--stack-name")

    sp = sub.add_parser("synth", help="write templates, state machine definitions and Dockerfiles")
    common(sp, aws=False)
    sp.add_argument("-o", "--out", type=Path, default=Path("sfnkit.out"))

    sp = sub.add_parser("deploy", help="build images and deploy both stacks")
    common(sp, aws=True)
    sp.add_argument(
        "--queue",
        action="append",
        default=[],
        metavar="NAME=ARN",
        help='bind a named BatchQueue("NAME") to a job queue ARN (repeatable)',
    )

    sp = sub.add_parser("run", help="start an execution of the deployed workflow")
    common(sp, aws=True)
    sp.add_argument("--input", help="JSON input, or @file.json")
    sp.add_argument("--no-wait", action="store_true", help="print the execution ARN and return")
    sp.add_argument("--timeout", type=float)

    sp = sub.add_parser("local", help="run the workflow in-process")
    common(sp, aws=False)
    sp.add_argument("--input", help="JSON input, or @file.json")

    sp = sub.add_parser("destroy", help="delete both stacks and the run artifacts")
    common(sp, aws=True)
    return p


def _session(args):
    import boto3

    return boto3.Session(region_name=args.region, profile_name=args.profile)


def _stack(args, app) -> str:
    from sfnkit.deploy.deployer import default_stack_name

    return args.stack_name or default_stack_name(app)


def cmd_synth(args) -> int:
    from sfnkit.build.dockerfile import render_dockerfile
    from sfnkit.compile.app import build_app
    from sfnkit.compile.cfn import assets_template, main_template

    app = build_app(load_workflow(args.target, args.project_dir), project_dir=args.project_dir)
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    (out / "template.json").write_text(json.dumps(main_template(app), indent=2))
    (out / "assets.json").write_text(json.dumps(assets_template(app), indent=2))
    for sm in app.state_machines:
        (out / f"{sm.name}.asl.json").write_text(json.dumps(sm.definition, indent=2))
    for image in app.images:
        (out / f"Dockerfile.{image.key}").write_text(render_dockerfile(image))
    print(f"wrote {out}/")
    return 0


def cmd_deploy(args) -> int:
    from sfnkit.compile.app import build_app
    from sfnkit.deploy.deployer import deploy

    queues = {}
    for spec in args.queue:
        name, sep, arn = spec.partition("=")
        if not sep or not name or not arn:
            raise SystemExit(f"--queue expects NAME=ARN, got {spec!r}")
        queues[name] = arn
    app = build_app(load_workflow(args.target, args.project_dir), project_dir=args.project_dir)
    res = deploy(app, stack_name=_stack(args, app), session=_session(args), queues=queues)
    print(json.dumps({"stack": res.stack_name, "state_machine": res.state_machine_arn, "bucket": res.bucket}))
    return 0


def cmd_run(args) -> int:
    from sfnkit.compile.app import build_app
    from sfnkit.deploy import client
    from sfnkit.deploy.deployer import stack_outputs

    app = build_app(load_workflow(args.target, args.project_dir), project_dir=args.project_dir)
    session = _session(args)
    outputs = stack_outputs(session.client("cloudformation"), _stack(args, app))
    arn = outputs["StateMachineArn"]
    payload = parse_input(args.input)
    express = outputs.get("StateMachineType") == "EXPRESS"
    if args.no_wait and not express:
        print(client.start(arn, payload, session=session))
        return 0
    result = client.run(arn, payload, express=express, timeout=args.timeout, session=session)
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_local(args) -> int:
    wf = load_workflow(args.target, args.project_dir)
    payload = parse_input(args.input)
    if not isinstance(payload, dict):
        raise SystemExit("--input for `local` must be a JSON object of workflow arguments")
    print(json.dumps(wf.local(**payload), indent=2, default=str))
    return 0


def cmd_destroy(args) -> int:
    from sfnkit.compile.app import build_app
    from sfnkit.deploy.deployer import destroy

    app = build_app(load_workflow(args.target, args.project_dir), project_dir=args.project_dir)
    destroy(app, stack_name=_stack(args, app), session=_session(args))
    return 0


COMMANDS = {
    "synth": cmd_synth,
    "deploy": cmd_deploy,
    "run": cmd_run,
    "local": cmd_local,
    "destroy": cmd_destroy,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return COMMANDS[args.command](args)
    except SfnkitError as e:
        print(f"sfnkit: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
