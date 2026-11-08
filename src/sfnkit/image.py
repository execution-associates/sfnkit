"""``Image``: what a step's container is built from.

Without ``base`` a step runs on the AWS Lambda Python base image, whatever its compute. With ``base`` it
runs on any Linux image with a POSIX shell: a BioContainers tool image, a CUDA image, GDAL, a plain
``debian:bookworm-slim``. sfnkit adds its runtime, the deps and the project code on top.

Python inside a custom base:
  * ``python=None`` (the default): the base's own ``python3`` if it is 3.12 or newer, so packages the
    base already has (pysam in a BioContainers image, say) stay importable. Otherwise sfnkit installs a
    standalone Python (the workflow's version) with uv.
  * ``python="base"``: the base's ``python3``, and the build fails if it is missing or older than 3.12.
  * ``python="3.13"``: a standalone Python of that version, whatever the base has.
  * With ``conda=[...]`` the packages and Python go into one conda env, created with micromamba from
    ``channels``. ``python`` then pins the env's Python (``None`` lets the solver pick 3.12 or newer).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

# Package specs end up in shell commands, so only allow what package managers actually use.
_SPEC = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+\-:=<>!~*,\[\] ]*$")
_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:@\-]*$")
_VERSION = re.compile(r"^3\.\d{1,2}(\.\d{1,2})?$")
DEFAULT_CHANNELS = ("conda-forge", "bioconda")


def _specs(what: str, values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, str):
        raise TypeError(f"{what} takes a list of package specs, not a string")
    out = tuple(values)
    for v in out:
        if not isinstance(v, str) or not _SPEC.match(v):
            raise ValueError(f"{what}: {v!r} doesn't look like a package spec")
    return out


@dataclass(frozen=True)
class Image:
    """A container image for steps. Deps and system packages add to the workflow's and the step's own.

    ``base``: an image reference (pin a digest, ``name@sha256:...``, for builds that never drift).
    ``deps``: pip requirements. ``system_packages``: installed with whichever of apt-get, apk, dnf,
    microdnf or yum the base has. ``conda``: conda package specs, from ``channels`` in that order.
    """

    base: str | None = None
    deps: Sequence[str] = ()
    system_packages: Sequence[str] = ()
    conda: Sequence[str] = ()
    channels: Sequence[str] = DEFAULT_CHANNELS
    python: str | None = None

    def __post_init__(self):
        set_ = lambda k, v: object.__setattr__(self, k, v)
        set_("deps", _specs("deps", self.deps))
        set_("system_packages", _specs("system_packages", self.system_packages))
        set_("conda", _specs("conda", self.conda))
        set_("channels", _specs("channels", self.channels))
        if self.base is not None and not _REF.match(self.base):
            raise ValueError(f"Image base {self.base!r} isn't an image reference")
        if self.base is None:
            if self.conda:
                raise ValueError(
                    "conda= needs a base image to build the env on, e.g. "
                    "Image('debian:bookworm-slim', conda=['samtools'])"
                )
            if self.python is not None:
                raise ValueError(
                    "python= is for custom bases; on the Lambda base set the version with @workflow(python=...)"
                )
        if self.python is not None and self.python != "base" and not _VERSION.match(self.python):
            raise ValueError(f'Image python must be None, "base" or a version like "3.13", not {self.python!r}')
        if self.python == "base" and self.conda:
            raise ValueError('python="base" can\'t combine with conda=; the env brings its own Python')
        if self.conda and not self.channels:
            raise ValueError("conda= needs at least one channel")
        if self.python not in (None, "base"):
            major, minor = (int(x) for x in self.python.split(".")[:2])
            if (major, minor) < (3, 12):
                raise ValueError("sfnkit needs Python 3.12 or newer inside the image")

    @property
    def custom(self) -> bool:
        return self.base is not None
