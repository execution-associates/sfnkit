"""A small sequencing-cohort pipeline on custom images and named Batch queues. ``examples/cohort_cdk.py``
deploys it with CDK, and the CDK e2e test runs it on real AWS.

  * ``make_samples`` lists the samples (a plain Lambda step).
  * ``qc.map(samples)`` is one Fargate Spot array job on a conda env with samtools from bioconda.
  * ``pileup`` runs on an EC2 queue whose instances have a 100 GiB scratch disk, and gets the whole
    manifest as an argument (the state machine stages it in S3, so it can be far bigger than Batch's
    request limits).
  * ``report`` is a Lambda function on ``python:3.13-slim`` instead of the Lambda base image.

The queues are names here; the CDK app binds them to real job queues.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

from sfnkit import Batch, BatchQueue, Image, Lambda, step, workflow

fargate = BatchQueue("fargate", assign_public_ip=True)  # public subnets, no NAT gateway
scratch = BatchQueue("scratch", platform="EC2")

samtools = Image("debian:bookworm-slim", conda=["samtools"])
tools = Image("debian:bookworm-slim", system_packages=["jq"])  # no Python of its own: sfnkit brings one
slim = Image("python:3.13-slim", deps=["pyyaml"])

SKIP = ["tests", "src", "scripts", "docs"]  # the project root holds more than this pipeline needs


@step
def make_samples(n: int) -> list[dict]:
    return [{"id": f"s{i}", "reads": i + 1} for i in range(n)]


@step(compute=Batch(queue=fargate, vcpus=0.5, memory=1024, timeout=900), image=samtools)
def qc(sample: dict) -> dict:
    """Count a sample's reads with samtools (a stand-in for real QC on a real BAM)."""
    header = "@HD\tVN:1.6\n@SQ\tSN:chr1\tLN:1000\n"
    reads = "".join(f"r{i}\t0\tchr1\t{i + 1}\t60\t4M\t*\t0\t0\tACGT\tIIII\n" for i in range(sample["reads"]))
    with tempfile.TemporaryDirectory() as d:
        sam = Path(d) / f"{sample['id']}.sam"
        sam.write_text(header + reads)
        count = subprocess.run(["samtools", "view", "-c", str(sam)], capture_output=True, text=True, check=True)
        version = subprocess.run(["samtools", "--version"], capture_output=True, text=True, check=True)
    return {"id": sample["id"], "reads": int(count.stdout), "samtools": version.stdout.split()[1]}


@step(compute=Batch(queue=scratch, vcpus=1, memory=1536, timeout=1800), image=tools)
def pileup(qcs: list, manifest: str) -> dict:
    """Combine the per-sample results; reports how much scratch disk the job had."""
    disk = shutil.disk_usage("/")
    jq = subprocess.run(["jq", "-n", "1 + 1"], capture_output=True, text=True, check=True)
    return {
        "samples": len(qcs),
        "reads": sum(q["reads"] for q in qcs),
        "samtools": sorted({q["samtools"] for q in qcs}),
        "manifest_bytes": len(manifest),
        "scratch_gib": round(disk.total / 2**30),
        "jq": int(jq.stdout),
    }


@step(compute=Lambda(memory=512), image=slim)
def report(summary: dict) -> dict:
    import yaml

    return {**summary, "yaml": yaml.safe_dump(summary, sort_keys=True)}


@workflow(exclude=SKIP)
def cohort(n: int = 3, manifest: str = ""):
    samples = make_samples(n)
    qcs = qc.map(samples)
    return report(pileup(qcs, manifest))


@step
def stamp(when: str) -> dict:
    return {"when": when}


@workflow(exclude=SKIP)
def ticker(when: str):
    """Started by an EventBridge schedule; ``when`` is the time the run was scheduled for."""
    return stamp(when)
