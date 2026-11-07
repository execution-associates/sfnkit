"""Assemble a Docker build context for one image variant.

Layout under ``<project>/.sfnkit/build/<variant key>/`` (never /tmp: contexts can be large and /tmp is
RAM on some hosts)::

    Dockerfile
    requirements.txt
    sfnkit/      the installed sfnkit package (works for PyPI and editable installs alike)
    project/     the import roots of the workflow module and its steps, merged

Only files git doesn't ignore go into ``project/``, so a ``.env`` or key file that's in ``.gitignore``,
``.git/info/exclude`` or the global excludes file never reaches the image. Inside a git work tree git
itself decides (``git ls-files``); elsewhere the ``.gitignore`` files are read directly.

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
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from sfnkit.app import App, ImageVariant
from sfnkit.build.dockerfile import docker_platform, render_dockerfile, render_requirements
from sfnkit.errors import DeployError

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
# .env files hold secrets so often that they stay out even when nothing ignores them, or git tracks them.
DEFAULT_EXCLUDE_GLOBS = ("*.pyc", "*.pyo", ".env", ".env.*")


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


def _excluded(rel: PurePosixPath, user_globs: tuple[str, ...]) -> bool:
    """The always-skipped names and ``exclude=`` globs, applied to a file and each folder above it."""
    for i, name in enumerate(rel.parts):
        prefix = "/".join(rel.parts[: i + 1])
        if (
            name in DEFAULT_EXCLUDE_NAMES
            or any(fnmatch.fnmatch(name, g) for g in DEFAULT_EXCLUDE_GLOBS)
            or any(fnmatch.fnmatch(prefix, g) or fnmatch.fnmatch(name, g) for g in user_globs)
        ):
            return True
    return False


def _git_files(root: Path) -> list[PurePosixPath] | None:
    """Files under ``root`` that git doesn't ignore: tracked ones plus untracked, non-ignored ones.

    None when git isn't installed or ``root`` isn't in a work tree."""
    git = shutil.which("git")
    if git is None:
        return None
    try:
        res = subprocess.run(
            [git, "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"]
            + [f"--exclude={name}" for name in sorted(DEFAULT_EXCLUDE_NAMES)]  # don't list a whole .venv
            + ["--", "."],
            capture_output=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    paths = {p.rstrip("/") for p in res.stdout.decode("utf-8", "surrogateescape").split("\0") if p}
    return [PurePosixPath(p) for p in sorted(paths)]


def _gitignore_files(root: Path, project: Path, seen: set[Path]) -> list[PurePosixPath]:
    """The same answer without git: walk ``root``, applying every ``.gitignore`` from ``project`` down."""
    import pathspec

    def load(d: Path) -> list[tuple[Path, pathspec.GitIgnoreSpec]]:
        f = d / ".gitignore"
        if not f.is_file():
            return []
        return [(d, pathspec.GitIgnoreSpec.from_lines(f.read_text(errors="surrogateescape").splitlines()))]

    def ignored(p: Path, is_dir: bool, specs) -> bool:
        verdict = False
        for base, spec in specs:  # shallowest first, so a deeper .gitignore has the last word
            rel = p.relative_to(base).as_posix() + ("/" if is_dir else "")
            hit = spec.check_file(rel)
            if hit.include is not None:
                verdict = hit.include
        return verdict

    specs: list = []
    if root != project and root.is_relative_to(project):
        d = project
        for part in root.relative_to(project).parts:
            specs += load(d)
            d = d / part

    out: list[PurePosixPath] = []

    def walk(d: Path, specs: list) -> None:
        real = d.resolve()
        if real in seen:  # a symlink back up the tree
            return
        seen.add(real)
        specs = specs + load(d)
        for entry in sorted(os.scandir(d), key=lambda e: e.name):
            p = Path(entry.path)
            is_dir = entry.is_dir()
            if entry.name in DEFAULT_EXCLUDE_NAMES or ignored(p, is_dir, specs):
                continue
            if is_dir:
                walk(p, specs)
            else:
                out.append(PurePosixPath(p.relative_to(root).as_posix()))

    walk(root, specs)
    return out


def project_files(root: Path, project: Path, _seen: set[Path] | None = None) -> list[PurePosixPath]:
    """Files under ``root`` (relative to it) that git, or failing that the .gitignore files, keeps."""
    seen = set() if _seen is None else _seen
    files = _git_files(root)
    if files is None:
        return _gitignore_files(root, project, seen)
    real = root.resolve()
    if real in seen:
        return []
    seen.add(real)
    out: list[PurePosixPath] = []
    for rel in files:
        p = root / rel
        if p.is_dir():  # a submodule, nested repository or symlinked folder: list it on its own terms
            out += [rel / sub for sub in project_files(p, p, seen)]
        elif p.exists():  # tracked files deleted from the work tree are still listed
            out.append(rel)
    return out


def _module_files(app: App, image: ImageVariant) -> list[tuple[Path, Path]]:
    """(import root, source file) for the workflow module and the modules of these steps."""
    project = app.project_dir.resolve()
    names = {app.module} | {fn.step.fn.__module__ for fn in app.functions if fn.image is image}
    out: list[tuple[Path, Path]] = []
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
        if root.is_relative_to(project):
            out.append((root, p))
    return out


def import_roots(app: App, image: ImageVariant) -> list[Path]:
    """Directories that must sit on sys.path for the workflow module and these steps to import."""
    roots: list[Path] = []
    for root, _ in _module_files(app, image):
        if root not in roots:
            roots.append(root)
    return roots or [app.project_dir.resolve()]


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
    roots = import_roots(app, image)
    for root in roots:
        for rel in project_files(root, app.project_dir.resolve()):
            if _excluded(rel, user_globs):
                continue
            dest = project_out / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / rel, dest)

    # An ancestor repository (a dotfiles repo in $HOME, say) can ignore the project wholesale. Better to
    # stop here than to ship an image that can't import its own steps.
    for root, file in _module_files(app, image):
        if root in roots and not (project_out / file.relative_to(root)).exists():
            raise DeployError(
                f"{file} is ignored by git (.gitignore, .git/info/exclude or core.excludesFile) or by "
                "exclude=, so it can't go into the image"
            )

    extra = os.pathsep.join([sfnkit_version(), image.python, image.architecture])
    return BuildContext(
        image=image,
        path=ctx_dir,
        tag=content_hash(ctx_dir, extra),
        platform=docker_platform(image.architecture),
    )
