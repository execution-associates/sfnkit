import base64
import importlib
import shutil
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


# ------------------------------------------------------------------------------- ignored files stay out

IGNORED = (".env", "secrets/token", "id.key")


def _shipped(ctx) -> set[str]:
    out = ctx.path / "project"
    return {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()}


def _ignore_fixture(code: Path, pkg: str) -> None:
    """A root .gitignore with a file, a folder, a glob and a negation, plus a nested one in the package."""
    _write(code, ".gitignore", ".env\nsecrets/\n*.key\n!public.key\n")
    _write(code, f"{pkg}/.gitignore", "local_settings.py\n")
    for rel in (*IGNORED, "public.key", f"{pkg}/local_settings.py"):
        _write(code, rel)


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


@pytest.fixture
def git_repo(tmp_path, monkeypatch):
    """Turn a project into a real repository, with git cut off from this machine's own config and given a
    global excludes file of the test's choosing."""
    if shutil.which("git") is None:
        pytest.skip("git isn't installed")
    excludes = tmp_path / "global-excludes"
    excludes.write_text(".envrc\n")
    (tmp_path / "gitconfig").write_text(f"[core]\n\texcludesFile = {excludes}\n")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")

    def init(root: Path) -> None:
        shutil.rmtree(root / ".git")
        _git(root, "init", "-q")

    return init


def test_context_leaves_out_what_git_ignores(project, git_repo):
    root, app, img = project("ctxpkg_d")
    git_repo(root)
    _ignore_fixture(root, "ctxpkg_d")
    _write(root, ".git/info/exclude", "scratch.txt\n")
    _write(root, "scratch.txt")
    _write(root, ".envrc", "export TOKEN=x")  # only the global excludes file ignores this
    _write(root, "tracked.key")
    _git(root, "add", "-f", "tracked.key")  # as in git, a tracked file counts even if a pattern matches it
    files = _shipped(prepare_context(app, img))
    assert {"ctxpkg_d/flows.py", "ctxpkg_d/__init__.py", "notes.txt", "public.key", "tracked.key"} <= files
    for gone in (*IGNORED, "ctxpkg_d/local_settings.py", "scratch.txt", ".envrc"):
        assert gone not in files
    assert not any(f.startswith(("data/", ".venv/", ".git/")) or ".pyc" in f for f in files)


def test_context_reads_gitignore_files_without_git(project, monkeypatch):
    root, app, img = project("ctxpkg_e")
    monkeypatch.setattr("sfnkit.build.context._git_files", lambda root: None)
    _ignore_fixture(root, "ctxpkg_e")
    files = _shipped(prepare_context(app, img))
    assert {"ctxpkg_e/flows.py", "notes.txt", "public.key"} <= files
    for gone in (*IGNORED, "ctxpkg_e/local_settings.py"):
        assert gone not in files
    assert not any(f.startswith(("data/", ".venv/", ".git/")) for f in files)


def test_context_without_git_applies_gitignore_above_the_import_root(project, monkeypatch):
    root, app, img = project("ctxpkg_f", src_layout=True)
    monkeypatch.setattr("sfnkit.build.context._git_files", lambda root: None)
    _write(root, ".gitignore", "*.key\n")  # in the project, above src/
    _write(root / "src", "ctxpkg_f/signing.key")
    files = _shipped(prepare_context(app, img))
    assert "ctxpkg_f/flows.py" in files and "ctxpkg_f/signing.key" not in files


def test_context_refuses_to_ship_without_the_workflow_code(project, git_repo):
    root, app, img = project("ctxpkg_g")
    git_repo(root)
    _write(root, ".gitignore", "ctxpkg_g/\n")
    with pytest.raises(DeployError, match=r"flows\.py is ignored by git"):
        prepare_context(app, img)


ENV_FILES = (".env", ".env.local", "ctxpkg_h/.env.production")


def test_context_never_ships_env_files_without_git(project, monkeypatch):
    root, app, img = project("ctxpkg_h")
    monkeypatch.setattr("sfnkit.build.context._git_files", lambda root: None)
    for rel in ENV_FILES:  # and no .gitignore at all
        _write(root, rel, "API_KEY=x")
    files = _shipped(prepare_context(app, img))
    assert "ctxpkg_h/flows.py" in files and not files & set(ENV_FILES)


def test_context_never_ships_env_files_even_when_tracked(project, git_repo):
    root, app, img = project("ctxpkg_i")
    git_repo(root)
    envs = [rel.replace("ctxpkg_h", "ctxpkg_i") for rel in ENV_FILES]
    for rel in envs:
        _write(root, rel, "API_KEY=x")
    _git(root, "add", "-f", *envs)
    files = _shipped(prepare_context(app, img))
    assert "ctxpkg_i/flows.py" in files and not files & set(envs)


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
