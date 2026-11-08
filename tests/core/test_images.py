"""sfnkit.Image: validation, which image a step gets, and the Dockerfiles for custom bases."""

from __future__ import annotations

import pytest

from sfnkit import Batch, BatchQueue, Image, step, workflow
from sfnkit.app import ImageVariant
from sfnkit.build.dockerfile import render_dockerfile, render_requirements
from sfnkit.compile.app import build_app

Q = BatchQueue("arn:aws:batch:us-west-2:123456789012:job-queue/q")
BIO = Image("quay.io/biocontainers/samtools:1.21--h96c455f_1", deps=["pysam"])
SHARED = Image("debian:bookworm-slim", system_packages=["jq"])


@step(compute=Batch(queue=Q), image=BIO, deps=["click"])
def align(x: int) -> int:
    return x


@step(compute=Batch(queue=Q))
def plain(x: int) -> int:
    return x


@step
def summarize(x: int) -> int:
    return x


@workflow(image=SHARED, deps=["httpx"])
def pipeline(x: int):
    return summarize(plain(align(x)))


@workflow
def no_images(x: int):
    return summarize(plain(x))


def _undecorated(x: int) -> int:
    return x


def _variant(**kw) -> ImageVariant:
    base = dict(key="k", deps=(), system_packages=(), python="3.14", architecture="x86_64", runtime="container")
    return ImageVariant(**{**base, **kw})


def test_validation():
    with pytest.raises(ValueError, match="needs a base image"):
        Image(conda=["samtools"])
    with pytest.raises(ValueError, match="custom bases"):
        Image(python="3.13")
    with pytest.raises(ValueError, match="3.12 or newer"):
        Image("debian:12", python="3.11")
    with pytest.raises(ValueError, match="package spec"):
        Image("debian:12", system_packages=["jq; rm -rf /"])
    with pytest.raises(TypeError, match="list of package specs"):
        Image("debian:12", deps="numpy")
    with pytest.raises(ValueError, match="image reference"):
        Image("not an image")
    with pytest.raises(ValueError, match="brings its own Python"):
        Image("debian:12", conda=["samtools"], python="base")
    with pytest.raises(TypeError, match="sfnkit.Image"):
        step(image="debian:12")(_undecorated)


def test_step_image_wins_and_deps_add_up():
    app = build_app(pipeline)
    by = {f.step.name: f.image for f in app.functions}
    assert by["align"].base == BIO.base
    assert set(by["align"].deps) == {"pysam", "click", "httpx"}  # image + step + workflow
    assert by["plain"].base == SHARED.base and by["plain"].system_packages == ("jq",)
    assert by["summarize"].base == SHARED.base and by["summarize"].runtime == "lambda"


def test_no_image_keeps_the_lambda_base_and_its_keys():
    app = build_app(no_images)
    assert all(not i.custom for i in app.images)
    df = render_dockerfile(next(i for i in app.images if i.runtime == "container"))
    assert df.splitlines()[1] == "FROM public.ecr.aws/lambda/python:3.14"


def test_custom_container_image():
    df = render_dockerfile(_variant(base="python:3.13-slim", deps=("pyyaml",), system_packages=("jq", "a=1.2")))
    lines = df.splitlines()
    assert lines[1:3] == ["FROM python:3.13-slim", "USER root"]
    assert "apt-get install -y --no-install-recommends jq a=1.2" in df and "apk add --no-cache jq" in df
    assert "uv pip install --no-cache --python /opt/sfnkit/bin/python --break-system-packages" in df
    assert "ENV PYTHONPATH=/opt/sfnkit/app" in df
    assert lines[-2:] == ['ENTRYPOINT ["/opt/sfnkit/bin/python", "-m", "sfnkit.runtime.container"]', "CMD []"]
    # auto: the base's own python3 when it is new enough, else uv's standalone build of the workflow's
    assert "sys.version_info < (3, 12)" in df and "uv python install --no-bin 3.14" in df


def test_python_modes():
    pinned = render_dockerfile(_variant(base="debian:12", python_mode="3.13"))
    assert "uv python install --no-bin 3.13" in pinned and "command -v" not in pinned
    base = render_dockerfile(_variant(base="debian:12", python_mode="base"))
    assert "uv python install" not in base and 'python=\\"base\\" but the base image has no python3' in base


def test_conda_env():
    df = render_dockerfile(_variant(base="debian:12", conda=("samtools=1.21", "bwa"), channels=("conda-forge", "bioconda")))
    assert "COPY --from=mambaorg/micromamba:" in df
    assert "-c conda-forge -c bioconda 'python>=3.12,<3.15' samtools=1.21 bwa" in df
    assert "--strict-channel-priority" in df and "/opt/sfnkit/activate.sh" in df
    assert "uv python install" not in df  # the env brings its own Python


def test_lambda_on_a_custom_base_uses_the_runtime_interface_client():
    img = _variant(base="python:3.13-slim", runtime="lambda", deps=("pyyaml",))
    df = render_dockerfile(img)
    assert 'ENTRYPOINT ["/opt/sfnkit/bin/python", "-m", "awslambdaric"]' in df
    assert 'CMD ["sfnkit.runtime.lambda_handler.handler"]' in df
    assert render_requirements(img).splitlines() == ["pyyaml", "boto3>=1.35", "awslambdaric>=3"]
    # the Lambda base ships boto3 and its own bootstrap
    assert render_requirements(_variant(runtime="lambda", deps=("pyyaml",))) == "pyyaml\n"
    # a pinned boto3 is left alone
    assert render_requirements(_variant(base="debian:12", deps=("boto3==1.40.0",))) == "boto3==1.40.0\n"
