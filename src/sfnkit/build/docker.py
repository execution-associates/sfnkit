"""Build an image variant and push it to ECR. ``Builder`` is the seam for other builders later."""

from __future__ import annotations

import base64
import subprocess
from collections.abc import Callable
from typing import Protocol

from sfnkit.build.context import BuildContext
from sfnkit.errors import DeployError


class Builder(Protocol):
    def ensure_image(self, context: BuildContext, repository_uri: str, ecr) -> str:
        """Make ``<repository_uri>:<context.tag>`` exist in ECR and return that URI."""
        ...


def image_exists(ecr, repository_name: str, tag: str) -> bool:
    try:
        resp = ecr.describe_images(repositoryName=repository_name, imageIds=[{"imageTag": tag}])
    except ecr.exceptions.ImageNotFoundException:
        return False
    return bool(resp.get("imageDetails"))


class DockerBuilder:
    def __init__(self, docker: str = "docker", run: Callable = subprocess.run, log: Callable = print):
        self.docker = docker
        self._run_fn = run
        self.log = log
        self._logged_in: set[str] = set()

    def _run(self, args: list[str], *, input: bytes | None = None) -> None:
        proc = self._run_fn(args, input=input, capture_output=True)
        if proc.returncode != 0:
            err = (proc.stderr or b"").decode(errors="replace").strip()[-4000:]
            raise DeployError(f"`{' '.join(args[:3])} ...` failed ({proc.returncode}):\n{err}")

    def login(self, ecr) -> None:
        data = ecr.get_authorization_token()["authorizationData"][0]
        endpoint = data["proxyEndpoint"]
        if endpoint in self._logged_in:
            return
        user, _, password = base64.b64decode(data["authorizationToken"]).decode().partition(":")
        self._run(
            [self.docker, "login", "--username", user, "--password-stdin", endpoint], input=password.encode()
        )
        self._logged_in.add(endpoint)

    def ensure_image(self, context: BuildContext, repository_uri: str, ecr) -> str:
        uri = f"{repository_uri}:{context.tag}"
        repo_name = repository_uri.split("/", 1)[1]
        if image_exists(ecr, repo_name, context.tag):
            self.log(f"image {context.image.key}: {context.tag} already in ECR, skipping build")
            return uri
        self.login(ecr)
        self.log(f"image {context.image.key}: building {context.tag} ({context.platform})")
        # --provenance=false: Lambda rejects the image index that build attestations create.
        self._run(
            [
                self.docker,
                "build",
                "--platform",
                context.platform,
                "--provenance=false",
                "-t",
                uri,
                str(context.path),
            ]
        )
        self.log(f"image {context.image.key}: pushing")
        self._run([self.docker, "push", uri])
        return uri
