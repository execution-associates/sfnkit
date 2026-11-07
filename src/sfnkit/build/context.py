"""Assemble a Docker build context for one image variant.

Layout under ``<project>/.sfnkit/build/<variant key>/`` (never /tmp: contexts can be large and /tmp is
RAM on some hosts)::

    Dockerfile
    requirements.txt
    sfnkit/      the installed sfnkit package (works for PyPI and editable installs alike)
    project/       the import roots of the workflow module and its steps, merged

The tag is a hash of everything in the context, so an unchanged project rebuilds to the same tag and the
push is skipped.
"""

from __future__ import annotations

import fnmatch
import hashlib
import importlib
import importlib.metadata
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from sfnkit.app import App, ImageVariant
from sfnkit.build.dockerfile import docker_platform, render_dockerfile, render_requirements

DEFAULT_EXCLUDE_NAMES = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".sfnkit",
        "node_modules",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".tox",
        ".idea",
        ".vscode",
        ".DS_Store",
    }
)
DEFAULT_EXCLUDE_GLOBS = ("*.pyc", "*.pyo")


@dataclass
class BuildContext:
    image: ImageVariant
    path: Path
    tag: str
    platform: str


def sfnkit_version() -> str:
    try:
        return importlib.metadata.version("sfnkit")
    except importlib.metadata.PackageNotFoundError:
        return "0+unknown"


def sfnkit_package_dir() -> Path:
    mod = importlib.import_module("sfnkit")
    return Path(next(iter(mod.__path__))).resolve()


def _ignore(root: Path, user_globs: tuple[str, ...]):
    def ignore(directory: str, names: list[str]) -> set[str]:
        rel_dir = Path(directory).resolve().relative_to(root)
        out = set()
        for name in names:
            rel = (rel_dir / name).as_posix()
            if (
                name in DEFAULT_EXCLUDE_NAMES
                or any(fnmatch.fnmatch(name, g) for g in DEFAULT_EXCLUDE_GLOBS)
                or any(fnmatch.fnmatch(rel, g) or fnmatch.fnmatch(name, g) for g in user_globs)
            ):
                out.add(name)
        return out

    return ignore


def import_roots(app: App, image: ImageVariant) -> list[Path]:
    """Directories that must sit on sys.path for the workflow module and these steps to import."""
    project = app.project_dir.resolve()
    names = {app.module} | {fn.step.fn.__module__ for fn in app.functions if fn.image is image}
    roots: list[Path] = []
    for name in sorted(names):
        if name == "__main__":
            continue
        mod = sys.modules.get(name) or importlib.import_module(name)
        file = getattr(mod, "__file__", None)
        if not file:
            continue
        p = Path(file).resolve()
        depth = name.count(".") + (1 if p.name == "__init__.py" else 0)
        root = p.parents[depth]
        if root.is_relative_to(project) and root not in roots:
            roots.append(root)
    return roots or [project]


def content_hash(path: Path, extra: str = "") -> str:
    h = hashlib.sha256(extra.encode())
    for f in sorted(p for p in path.rglob("*") if p.is_file()):
        h.update(f.relative_to(path).as_posix().encode() + b"\0")
        h.update(f.read_bytes())
        h.update(b"\0")
    return h.hexdigest()[:16]


def prepare_context(app: App, image: ImageVariant) -> BuildContext:
    ctx_dir = app.project_dir.resolve() / ".sfnkit" / "build" / image.key
    if ctx_dir.exists():
        shutil.rmtree(ctx_dir)
    ctx_dir.mkdir(parents=True)
    (ctx_dir / "Dockerfile").write_text(render_dockerfile(image))
    (ctx_dir / "requirements.txt").write_text(render_requirements(image))

    pkg = sfnkit_package_dir()
    shutil.copytree(pkg, ctx_dir / "sfnkit", ignore=_ignore(pkg, ()))

    project_out = ctx_dir / "project"
    project_out.mkdir()
    user_globs = tuple(app.exclude)
    for root in import_roots(app, image):
        shutil.copytree(root, project_out, dirs_exist_ok=True, ignore=_ignore(root, user_globs))

    extra = os.pathsep.join([sfnkit_version(), image.python, image.architecture])
    return BuildContext(
        image=image,
        path=ctx_dir,
        tag=content_hash(ctx_dir, extra),
        platform=docker_platform(image.architecture),
    )
