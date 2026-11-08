"""Build custom-base images with Docker for real and run a step inside each (marker ``docker``)."""

from __future__ import annotations

import json
import shutil
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest

from sfnkit.build.context import prepare_context
from sfnkit.compile.app import build_app

pytestmark = pytest.mark.docker
ROOT = Path(__file__).resolve().parent.parent
RIE = "https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie"


def _docker(*args: str, check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check, **kw)


@pytest.fixture(scope="module")
def built():
    if not shutil.which("docker"):
        pytest.skip("needs docker")
    import tests.image_flows as flows

    app = build_app(flows.images, project_dir=ROOT)
    tags = {}
    for fn in app.functions:
        ctx = prepare_context(app, fn.image)
        tag = f"sfnkit-test-{fn.step.name.replace('_', '-')}:{ctx.tag}"
        proc = _docker("build", "--platform", ctx.platform, "-t", tag, str(ctx.path), check=False)
        assert proc.returncode == 0, proc.stderr[-3000:]
        tags[fn.step.name] = tag
    yield tags
    _docker("rmi", "-f", *tags.values(), check=False)


def _run_step(tag: str, step: str, x: int = 1) -> dict:
    """Run a step through the runtime's own ``execute``, inside the built image."""
    envelope = {"sw": 1, "step": f"tests.image_flows.{step}", "execution": "e", "node": "n", "args": {"x": x}}
    code = (
        "import json, sys; from sfnkit.runtime.execute import execute; "
        f"print(json.dumps(execute(json.loads({json.dumps(json.dumps(envelope))}))))"
    )
    proc = _docker("run", "--rm", "--entrypoint", "/opt/sfnkit/bin/python", tag, "-c", code, check=False)
    assert proc.returncode == 0, proc.stderr[-3000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])["v"]


def test_base_python_is_reused(built):
    out = _run_step(built["on_slim"], "on_slim")
    assert out["yaml"] == 2 and out["version"] == [3, 13]
    assert out["python"].startswith("/opt/sfnkit/bin/python")


def test_standalone_python_when_the_base_has_none(built):
    out = _run_step(built["no_python"], "no_python")
    assert out["jq"] == 2 and out["version"] >= [3, 12]  # debian's own python3 is 3.11, so uv's is used


def test_conda_env_with_tools_on_path(built):
    out = _run_step(built["with_conda"], "with_conda")
    assert out["samtools"].startswith("samtools ") and out["on_path"] == "/opt/sfnkit/env/bin/samtools"


def test_alpine_musl_with_a_pinned_python(built):
    out = _run_step(built["on_alpine"], "on_alpine")
    assert out["version"] == [3, 13] and out["jq"]


def test_container_entrypoint(built):
    inspect = json.loads(_docker("image", "inspect", built["no_python"]).stdout)[0]["Config"]
    assert inspect["Entrypoint"] == ["/opt/sfnkit/bin/python", "-m", "sfnkit.runtime.container"]


def test_lambda_on_a_custom_base_answers_invocations(built, tmp_path):
    """Lambda's runtime interface emulator in front of the image's awslambdaric entrypoint."""
    rie = ROOT / ".sfnkit" / "aws-lambda-rie"
    if not rie.exists():
        rie.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(RIE, rie)
        rie.chmod(0o755)
    tag = built["lambda_custom"]
    entry = json.loads(_docker("image", "inspect", tag).stdout)[0]["Config"]
    assert entry["Entrypoint"] == ["/opt/sfnkit/bin/python", "-m", "awslambdaric"]
    name = f"sfnkit-rie-{int(time.time())}"
    _docker(
        "run", "-d", "--name", name, "-p", "127.0.0.1:19000:8080", "-v", f"{rie}:/aws-lambda-rie:ro",
        "--entrypoint", "/aws-lambda-rie", tag, *entry["Entrypoint"], *entry["Cmd"],
    )
    try:
        envelope = {"sw": 1, "step": "tests.image_flows.lambda_custom", "execution": "e", "node": "n",
                    "args": {"x": 1}}
        url = "http://127.0.0.1:19000/2015-03-31/functions/function/invocations"
        for _ in range(50):
            try:
                req = urllib.request.Request(url, data=json.dumps(envelope).encode(), method="POST")
                body = json.loads(urllib.request.urlopen(req, timeout=10).read())
                break
            except OSError:
                time.sleep(0.2)
        else:
            raise AssertionError(_docker("logs", name, check=False).stderr[-3000:])
        assert body["v"]["yaml"] == 3 and body["v"]["version"] == [3, 13], body
    finally:
        _docker("rm", "-f", name, check=False)


def test_python_base_mode_fails_the_build_clearly():
    if not shutil.which("docker"):
        pytest.skip("needs docker")
    import tests.image_flows as flows

    app = build_app(flows.base_python_missing, project_dir=ROOT)
    ctx = prepare_context(app, app.functions[0].image)
    proc = _docker("build", "--platform", ctx.platform, "-t", "sfnkit-test-base:x", str(ctx.path), check=False)
    assert proc.returncode != 0
    assert 'python="base" but the base image has no python3 >= 3.12' in proc.stderr
