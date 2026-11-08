"""Workflows for the CDK tests: a named EC2 queue, a named Fargate queue and a custom image."""

from sfnkit import Batch, BatchQueue, Image, step, workflow

gpu = BatchQueue("gpu", platform="EC2")
cpu = BatchQueue("cpu")
tools = Image("debian:bookworm-slim", system_packages=["jq"], deps=["pyyaml"])


@step(compute=Batch(queue=gpu, vcpus=4, memory=16384, gpus=1), image=tools)
def train(sample: str) -> str:
    return sample


@step(compute=Batch(queue=cpu, vcpus=1, memory=2048))
def prep(sample: str) -> str:
    return sample


@step
def report(models: list) -> int:
    return len(models)


@workflow
def cohort(samples: list, day: str = "today"):
    ready = prep.map(samples)
    models = train.map(ready)
    return report(models)
