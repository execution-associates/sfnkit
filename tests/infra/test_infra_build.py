import base64
import importlib
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from sfnkit.app import App, AppSettings, FunctionSpec, ImageVariant
from sfnkit.build.context import prepare_context
from sfnkit.build.docker import DockerBuilder
from sfnkit.build.dockerfile import UV_VERSION, render_dockerfile, render_requirements
from sfnkit.compute.lambda_ import Lambda
from sfnkit.errors import DeployError
from sfnkit.iam import Policy

# ------------------------------------------------------------------------------------------ dockerfile


def test_dockerfile_with_deps_and_system_packages():
    img = ImageVariant("k1", ("pyyaml>=6", "httpx"), ("git", "jq"), "3.14", "x86_64")
    df = render_dockerfile(img)
    assert "FROM public.ecr.aws/lambda/python:3.14" in df
    assert "RUN microdnf install -y git jq && microdnf clean all" in df
    assert f"COPY --from=ghcr.io/astral-sh/uv:{UV_VERSION} /uv /bin/uv" in df
    assert "--python-platform x86_64-manylinux_2_28" in df and "--python-version 3.14" in df
    assert '--target "${LAMBDA_TASK_ROOT}"' in df
    assert "COPY sfnkit/ ${LAMBDA_TASK_ROOT}/sfnkit/" in df
    assert 'CMD ["sfnkit.runtime.lambda_handler.handler"]' in df
    assert render_requirements(img) == "pyyaml>=6\nhttpx\n"


def test_dockerfile_minimal_arm():
    df = render_dockerfile(ImageVariant("k2", (), (), "3.13", "arm64"))
    assert "microdnf" not in df and "requirements.txt" not in df
    assert "python:3.13" in df
    with pytest.raises(ValueError):
        render_dockerfile(ImageVariant("k3", (), (), "3.14", "sparc"))


# --------------------------------------------------------------------------------------------- context


def _write(root: Path, rel: str, text: str = "x") -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A throwaway project with one step module, importable for the duration of the test."""

    def make(pkg: str, src_layout: bool = False):
        root = tmp_path / "proj"
        code = root / "src" if src_layout else root
        _write(code, f"{pkg}/__init__.py", "")
        _write(
            code,
            f"{pkg}/flows.py",
            textwrap.dedent(
                """
                from sfnkit.step import step

                @step
                def work(x):
                    return x
                """
            ),
        )
        _write(root, ".git/config")
        _write(root, ".venv/lib/thing.py")
        _write(root, "data/big.csv", "1,2,3")
        _write(root, "notes.txt", "hello")
        _write(code, f"{pkg}/__pycache__/flows.cpython-314.pyc")
        monkeypatch.syspath_prepend(str(code))
        mod = importlib.import_module(f"{pkg}.flows")
        img = ImageVariant("ctx00001", ("pyyaml",), (), "3.14", "x86_64")
        fn = FunctionSpec("FnWork", mod.work, Lambda(), Policy(), img)
        app = App(
            name="ctx",
            module=f"{pkg}.flows",
            workflow=SimpleNamespace(name="ctx"),
            functions=[fn],
            state_machines=[],
            images=[img],
            settings=AppSettings(),
            project_dir=root,
            exclude=("data/*",),
        )
        return root, app, img

    yield make
    for name in list(sys.modules):
        if name.startswith("ctxpkg"):
            del sys.modules[name]


def test_context_layout_and_exclusions(project):
    root, app, img = project("ctxpkg_a")
    ctx = prepare_context(app, img)
    assert ctx.path == root.resolve() / ".sfnkit" / "build" / img.key
    files = {p.relative_to(ctx.path).as_posix() for p in ctx.path.rglob("*") if p.is_file()}
    assert {"Dockerfile", "requirements.txt", "project/ctxpkg_a/flows.py", "project/notes.txt"} <= files
    assert "sfnkit/runtime/lambda_handler.py" in files
    assert not any(f.startswith(("project/.git", "project/.venv", "project/data")) for f in files)
    assert not any("__pycache__" in f or f.endswith(".pyc") for f in files)
    assert ctx.platform == "linux/amd64" and len(ctx.tag) == 16


def test_context_hash_is_stable_and_content_sensitive(project):
    root, app, img = project("ctxpkg_b")
    t1 = prepare_context(app, img).tag
    assert prepare_context(app, img).tag == t1  # the rebuilt context doesn't include itself
    (root / "notes.txt").write_text("changed")
    assert prepare_context(app, img).tag != t1
    (root / "notes.txt").write_text("hello")
    (root / "data" / "big.csv").write_text("excluded files don't matter")
    assert prepare_context(app, img).tag == t1


def test_context_src_layout_puts_packages_at_task_root(project):
    _, app, img = project("ctxpkg_c", src_layout=True)
    ctx = prepare_context(app, img)
    assert (ctx.path / "project" / "ctxpkg_c" / "flows.py").exists()
    assert not (ctx.path / "project" / "notes.txt").exists()  # only the import root is shipped


# ---------------------------------------------------------------------------------------------- docker


class FakeEcr:
    class exceptions:
        class ImageNotFoundException(Exception):
            pass

    def __init__(self, existing=()):
        self.existing = set(existing)

    def describe_images(self, repositoryName, imageIds):
        tag = imageIds[0]["imageTag"]
        if tag not in self.existing:
            raise self.exceptions.ImageNotFoundException()
        return {"imageDetails": [{"imageTags": [tag]}]}

    def get_authorization_token(self):
        token = base64.b64encode(b"AWS:secretpw").decode()
        return {"authorizationData": [{"authorizationToken": token, "proxyEndpoint": "https://1.dkr.ecr"}]}


def _ctx(tmp_path):
    from sfnkit.build.context import BuildContext

    return BuildContext(ImageVariant("k", (), (), "3.14", "arm64"), tmp_path, "tag123", "linux/arm64")


def test_docker_builder_builds_and_pushes(tmp_path):
    calls = []

    def run(args, input=None, capture_output=True):
        calls.append((args, input))
        return subprocess.CompletedProcess(args, 0, b"", b"")

    repo = "1.dkr.ecr.us-west-2.amazonaws.com/repo"
    uri = DockerBuilder(run=run, log=lambda *a: None).ensure_image(_ctx(tmp_path), repo, FakeEcr())
    assert uri == f"{repo}:tag123"
    login, build, push = (c[0] for c in calls)
    assert login[:2] == ["docker", "login"] and calls[0][1] == b"secretpw"
    assert build[:2] == ["docker", "build"] and "--provenance=false" in build
    assert build[build.index("--platform") + 1] == "linux/arm64"
    assert push == ["docker", "push", uri]


def test_docker_builder_skips_existing_tag(tmp_path):
    def run(*a, **k):
        raise AssertionError("must not run docker")

    repo = "1.dkr.ecr.us-west-2.amazonaws.com/repo"
    out = DockerBuilder(run=run, log=lambda *a: None).ensure_image(_ctx(tmp_path), repo, FakeEcr({"tag123"}))
    assert out.endswith(":tag123")


def test_docker_builder_failure_raises_deploy_error(tmp_path):
    def run(args, input=None, capture_output=True):
        rc = 1 if args[1] == "build" else 0
        return subprocess.CompletedProcess(args, rc, b"", b"no space left")

    with pytest.raises(DeployError, match="no space left"):
        DockerBuilder(run=run, log=lambda *a: None).ensure_image(_ctx(tmp_path), "1.dkr/repo", FakeEcr())
