"""Steps on custom base images, built for real by tests/test_images_docker.py."""

import shutil
import subprocess
import sys

from sfnkit import Batch, BatchQueue, Image, Lambda, step, workflow

Q = BatchQueue("arn:aws:batch:us-west-2:123456789012:job-queue/images")


def _where() -> dict:
    return {"python": sys.executable, "version": list(sys.version_info[:2])}


@step(compute=Batch(queue=Q), image=Image("python:3.13-slim", deps=["pyyaml"]))
def on_slim(x: int) -> dict:
    import yaml

    return {**_where(), "yaml": yaml.safe_load("a: 1")["a"] + x}


@step(compute=Batch(queue=Q), image=Image("debian:bookworm-slim", system_packages=["jq"]))
def no_python(x: int) -> dict:
    out = subprocess.run(["jq", "-n", f"{x} + 1"], capture_output=True, text=True, check=True).stdout
    return {**_where(), "jq": int(out)}


@step(compute=Batch(queue=Q), image=Image("debian:bookworm-slim", conda=["samtools"]))
def with_conda(x: int) -> dict:
    out = subprocess.run(["samtools", "--version"], capture_output=True, text=True, check=True).stdout
    return {**_where(), "samtools": out.splitlines()[0], "on_path": shutil.which("samtools")}


@step(compute=Batch(queue=Q), image=Image("alpine:3.20", system_packages=["jq"], python="3.13"))
def on_alpine(x: int) -> dict:
    return {**_where(), "jq": shutil.which("jq")}


@step(compute=Lambda(), image=Image("python:3.13-slim", deps=["pyyaml"]))
def lambda_custom(x: int) -> dict:
    import yaml

    return {**_where(), "yaml": yaml.safe_load("b: 2")["b"] + x}


@workflow
def images(x: int = 1):
    return [on_slim(x), no_python(x), with_conda(x), on_alpine(x), lambda_custom(x)]


@step(compute=Batch(queue=Q), image=Image("debian:bookworm-slim", python="base"))
def needs_base_python(x: int) -> int:
    return x


@workflow
def base_python_missing(x: int = 1):
    return needs_base_python(x)
