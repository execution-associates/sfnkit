# sfnkit

Write AWS Step Functions workflows as plain Python. Steps are decorated functions that run as Lambda
functions or AWS Batch jobs, from container images. The workflow body is ordinary Python, including
`match`, `if` and `with` blocks, and is compiled into a state machine.

```
pip install sfnkit            # sfnkit deploy
pip install "sfnkit[cdk]"     # or as constructs in a CDK app
```

Requires Python 3.12+. Deploying also needs Docker (to build the images) and AWS credentials that can
create IAM roles.

Here is a complete workflow, `examples/checkout.py`. It loads an order from S3, prices each line,
scores the order's risk, and charges the card:

```python
# examples/checkout.py
import json

import boto3

from sfnkit import Allow, Err, Lambda, Ok, Policy, Retry, attempt, fail, step, subworkflow, wait, workflow

BUCKET = "acme-orders"  # point this at your own bucket

# The workflow's base IAM set. A step without its own iam= gets exactly this.
base = Policy(Allow("s3:GetObject", f"arn:aws:s3:::{BUCKET}/orders/*"))


class PaymentDeclined(Exception):
    pass


@step
def load_order(key: str) -> dict:
    body = boto3.client("s3").get_object(Bucket=BUCKET, Key=key)["Body"]
    return json.load(body)  # {"id": "o-17", "card": "4242...", "items": [{"sku": "mug", "qty": 2}]}


@step(
    deps=["pyyaml>=6"],  # added to this step's image only
    iam=Policy(Allow("s3:GetObject", f"arn:aws:s3:::{BUCKET}/prices.yaml")),  # this, and none of base
)
def load_prices() -> dict:
    import yaml

    body = boto3.client("s3").get_object(Bucket=BUCKET, Key="prices.yaml")["Body"]
    return yaml.safe_load(body)  # {"mug": 12.5, "lamp": 300}


@step
def price_item(item: dict, prices: dict) -> float:
    return prices[item["sku"]] * item["qty"]


@step
def order_total(line_totals: list[float]) -> float:
    return round(sum(line_totals), 2)


@step
def score_risk(total: float) -> str:
    return "low" if total < 100 else "medium" if total < 1000 else "high"  # stand-in for a fraud model


@step(compute=Lambda(timeout=120), retry=Retry(on=[TimeoutError], max_attempts=3))
def charge(card: str, amount: float) -> str:
    if card.startswith("4000"):  # stand-in for a payment provider
        raise PaymentDeclined(f"card ending {card[-4:]} declined")
    return f"rcpt-{card[-4:]}-{amount}"


@step(iam=base + Allow("s3:PutObject", f"arn:aws:s3:::{BUCKET}/declined/*"))  # base, plus this
def record_decline(order: dict, cause: dict) -> None:
    body = json.dumps({"order": order["id"], "error": cause["errorMessage"]})
    boto3.client("s3").put_object(Bucket=BUCKET, Key=f"declined/{order['id']}.json", Body=body)


@workflow(iam=base)
def checkout(key: str):
    order = load_order(key)  # these two don't depend on each other,
    prices = load_prices()  # so they run side by side in a Parallel state
    line_totals = price_item.map(order["items"], prices=prices, max_concurrency=10)  # Map state

    with subworkflow("risk"):  # a child state machine
        total = order_total(line_totals)  # sum() on a step's output has to happen in a step
        risk = score_risk(total)

    match risk:  # Choice state
        case "low":
            pass
        case "medium" if total < 500:
            wait(seconds=300)  # Wait state: hold the order for 5 minutes
        case _:
            fail("HighRisk", "order needs manual review")  # Fail state

    match attempt(charge, order["card"], total):  # Catch
        case Ok(receipt):
            pass
        case Err("PaymentDeclined", cause):  # any other error fails the workflow
            record_decline(order, cause)
            receipt = None

    return {"order": order["id"], "total": total, "receipt": receipt}
```

**Every function the workflow calls is a `@step`.** Each step deploys as its own Lambda function with
its own IAM role, and runs as its own Task state. Steps with the same dependencies share an image.

**The workflow body only wires steps together.** It is traced when you synth or deploy and never runs
in AWS. So anything that computes on a step's output, like the `sum()` in `order_total`, has to happen
inside a step. A plain function (no `@step`) called from the body just runs during tracing: it can pass
values along, but it can't compute on them.

**Outside a workflow, a step is a plain function**, so steps unit-test like any Python:
`price_item({"sku": "mug", "qty": 2}, {"mug": 12.5})` returns `25.0`. `checkout.local(key)` runs the
whole workflow in-process, calling the real step code. `tests/test_readme_example.py` does that for
every path against a mocked S3.

```
sfnkit local   examples.checkout:checkout --input '{"key": "orders/o-17.json"}'
sfnkit synth   examples.checkout:checkout -o out/
sfnkit deploy  examples.checkout:checkout --region us-west-2
sfnkit run     examples.checkout:checkout --input '{"key": "orders/o-17.json"}'
sfnkit destroy examples.checkout:checkout
```

`local` runs in-process but its steps still call AWS, so it needs the bucket to exist.

## How a workflow body becomes a state machine

A step called inside a `@workflow` doesn't run. It returns a *ref* to its future output, and the call
is recorded. Called anywhere else, a step is a normal Python function.

A condition on a ref (`==`, `<`, `in`, `and`, `if x:`) is a *decision*. The tracer runs the workflow
body once per combination of decisions and merges the runs into one graph:

- **Calls that don't depend on each other** run in one Parallel state.
- **Each decision** becomes a Choice state.
- **A variable assigned in several branches** is merged automatically, so it can be used after the
  `if`/`match` like normal Python.

Because the body runs once per path, it must be deterministic and free of side effects. Paths are
capped at `max_paths` (default 1024). Each `@workflow` is traced on its own, so splitting a large
body into child workflows keeps the count down.

## Control flow

| Python | Step Functions |
|---|---|
| independent step calls | Parallel |
| `if/elif/else`, `match` with literal, value and OR patterns and guards | Choice |
| `step.map(items, **kw)`, `with each(items) as it:` | Map (inline), or Distributed with `distributed=True`. On a Batch step, `step.map` is one array job |
| `match attempt(fn, ...)` with `case Ok(v)` / `case Err(name, cause)` | Catch |
| `with subworkflow("name"):`, calling another `@workflow` | child state machine (`startExecution.sync:2`) |
| `wait(seconds=...)`, `wait(until=...)` | Wait. A barrier: everything before it runs first |
| `fail(error, cause)` | Fail |

**`attempt(fn, *args)`** runs a step, a workflow, or a plain function containing several steps.

- **It catches only the error names its `match` handles.** A Python exception's name is its class
  name, so `Err(errors.PaymentDeclined, c)` and `Err("PaymentDeclined", c)` match the same error.
- **Errors no case handles fail the workflow,** just as they would without `attempt`.
- **`case Err(error, cause):` catches everything.** `cause` is Lambda's error object: `errorMessage`,
  `errorType` and `stackTrace`.

**`each(items)`** takes items from a workflow input, a literal list, a step's output (or a field of
it), or another inline map's results. A Distributed Map can also read straight from S3 with
`S3Objects(bucket, prefix)` or `S3File(bucket, key, format="json"|"jsonl"|"csv")`. Inside the block,
`it.item` is the current item and `it.result(x)` sets that item's result. After the block, `it.value`
is the list of results.

**Checked when the decorator runs**, because tracing would silently take the wrong branch otherwise:

- `case None/True/False` and `is` compare by identity. Use `is_null(x)`.
- List and dict patterns aren't supported. Neither are class patterns other than `Ok`/`Err`.
- A bare exception name inside `Err(...)` captures every error instead of matching that exception.
  Write the dotted name or a string.
- `try/except` isn't supported; use `attempt`. Neither is `while`.
- `return` must be the last statement of the workflow.

**Not supported** in a workflow body: arithmetic on refs, `len()` (use `length(x)`), and iterating a
ref (use `.map`/`each`). Do those in a step.

## IAM

Every step's role gets three layers:

1. **The runtime baseline.** This is added automatically: its own logs, plus read and write on the
   artifacts bucket under `runs/`.
2. **The workflow's base set**, from `@workflow(iam=base)` or `subworkflow(..., iam=base)`. Steps
   with no `iam=` inherit the nearest one.
3. **The step's own `iam=`, which replaces the base set.**
   - Write `iam=base + Allow(...)` to keep the base set and add to it.
   - Write `iam=Policy(Allow(...))` for granular permissions with none of the base set.

`Policy` objects compose with `+`, which takes their union and removes duplicates. A step that ends
up under two different policies is deployed as two functions. A reusable `@workflow` uses its own
`iam=`. Resources may be CloudFormation intrinsics, for example
`{"Fn::Sub": "arn:${AWS::Partition}:s3:::${ArtifactsBucket}/fixtures/*"}`.

## Data between steps

Step arguments and results must be JSON-serializable. Step Functions caps each state's input and
output at 256 KiB, so any value over `offload_threshold` (default 32 KiB) goes to the artifacts
bucket. Step code only ever sees plain values.

A condition is evaluated inside Step Functions, which can't read S3. So the step that produces a
value also returns, inline, the specific fields conditions read (*facts*). Conditions can therefore
branch on a value even when the rest of it went to S3.

The wire format is in `src/sfnkit/runtime/protocol.py`.

## Dependencies and images

A step's image is built from the workflow's `deps` + the step's `deps` (+ `system_packages`,
installed with `microdnf`), on `public.ecr.aws/lambda/python:<python>`. Steps with the same set
share an image. Tags are content hashes, so an unchanged image isn't rebuilt or pushed again. Build
contexts go in `<project>/.sfnkit/build/`.

**The image gets the folders your workflow and steps import from, minus anything git ignores.**

- **Inside a git repository, git decides.** Nested `.gitignore` files, `.git/info/exclude` and your
  global excludes file all count. As in git, a tracked file is included even if a pattern matches it.
- **Outside a repository, sfnkit reads the `.gitignore` files itself.**
- **`.env` and `.env.*` files, and caches such as `.venv` and `__pycache__`, are always left out**,
  even when nothing ignores them or git tracks them. `@workflow(exclude=[...])` takes more globs.

Everything else ships, so keep other secrets, like a key file, out of those folders or ignore them.
The build stops with an error if the ignore rules would drop the workflow's own code.

### Custom base images

`Image(base=...)` runs a step on any Linux image with a shell instead of the Lambda base: a
BioContainers tool, a CUDA image, GDAL, plain Debian. sfnkit adds Python if needed, the deps, its
runtime and your code on top.

```python
from sfnkit import Batch, BatchQueue, Image, Lambda, step

gpu = BatchQueue("gpu", platform="EC2")  # bound to a real queue at deploy time
cuda = Image("nvidia/cuda:12.6.3-runtime-ubuntu24.04", deps=["torch"], system_packages=["git"])
bio = Image("debian:bookworm-slim", conda=["samtools=1.21", "bcftools"])  # bioconda + conda-forge
gdal = Image("ghcr.io/osgeo/gdal:ubuntu-small-3.10.0", deps=["rasterio"])

@step(compute=Batch(queue=gpu, gpus=1, vcpus=8, memory=61440), image=cuda)
def train(shard: str) -> str: ...

@step(compute=Batch(queue=gpu, vcpus=4, memory=8192), image=bio)
def call_variants(bam: str) -> str: ...

@step(compute=Lambda(memory=2048), image=gdal)  # Lambda works too
def reproject(tile: str) -> str: ...
```

- **Which Python.** By default, the base's own `python3` if it's 3.12 or newer, so packages already
  in the image stay importable. Otherwise sfnkit installs a standalone Python of the workflow's
  version with uv. `python="base"` insists on the base's (the build fails without one) and
  `python="3.13"` always uses a standalone one.
- **`conda=[...]`** creates a conda env with micromamba from `channels` (default `conda-forge`, then
  `bioconda`), with Python in it. The env is activated for every run, so its tools are on `PATH`. Its
  Python is the newest the solver can fit, up to the workflow's version, unless `python=` pins it.
- **`system_packages`** are installed with whichever of `apt-get`, `apk`, `dnf`, `microdnf` or `yum`
  the base has. **`deps`** are pip requirements, installed into that Python's own environment.
- **Deps add up.** An image's `deps` and `system_packages` join the workflow's and the step's.
  `@workflow(image=...)` sets the default image for its steps.
- **Lambda on a custom base** gets `awslambdaric`, AWS's runtime interface client, so the base must
  use glibc (not Alpine's musl).
- **The build runs as root**, and so do Batch jobs on these images. Pin a digest
  (`name@sha256:...`) for builds that never drift.

`tests/test_images_docker.py` builds and runs each kind for real: `uv run pytest -m docker`.

## Deploying

`deploy` creates two stacks:

- `<stack>-assets`: the ECR repository and the artifacts bucket.
- `<stack>`: the functions, their roles, log groups and state machines.

`destroy` deletes both, emptying the bucket first. The deploying credentials must be allowed to
create IAM roles. Credentials from `sts get-session-token` can't call IAM without MFA, so use
long-term or assumed-role credentials.

A named Batch queue (`BatchQueue("gpu")`) is bound at deploy time with `sfnkit deploy ... --queue
gpu=arn:aws:batch:...`.

## Deploying with CDK

`sfnkit.cdk.SfnkitWorkflow` puts a workflow into a CDK stack, so `cdk deploy` ships it with the rest of
your infrastructure:

```python
import aws_cdk as cdk
from aws_cdk import aws_batch as batch, aws_ec2 as ec2, aws_iam as iam
from sfnkit.cdk import SCHEDULED_TIME, Ec2Queue, SfnkitWorkflow

# examples/cohort.py: its steps run on BatchQueue("fargate") and BatchQueue("scratch", platform="EC2")
from examples.cohort import cohort, ticker

class Pipeline(cdk.Stack):
    def __init__(self, scope, id, **kw):
        super().__init__(scope, id, **kw)
        vpc = ec2.Vpc.from_lookup(self, "Vpc", is_default=True)
        fargate_env = batch.FargateComputeEnvironment(self, "FargateEnv", vpc=vpc, spot=True)
        fargate = batch.JobQueue(self, "Fargate", compute_environments=[
            batch.OrderedComputeEnvironment(compute_environment=fargate_env, order=1)])
        scratch = Ec2Queue(self, "Scratch", vpc=vpc, scratch_gib=500)

        wf = SfnkitWorkflow(self, "Cohort", workflow=cohort, queues={"fargate": fargate, "scratch": scratch})
        analysts = iam.Role(self, "Analysts", assumed_by=iam.AccountRootPrincipal())
        wf.state_machine.grant_start_execution(analysts)  # an ordinary sfn.IStateMachine

        tick = SfnkitWorkflow(self, "Ticker", workflow=ticker)
        tick.add_schedule("Daily", "cron(0 2 * * ? *)", input={"when": SCHEDULED_TIME}, timezone="UTC")

app = cdk.App()
Pipeline(app, "cohort", env=cdk.Environment(account="123456789012", region="us-west-2"))
app.synth()
```

- **It's the same stack `sfnkit deploy` makes**, as native resources of yours: functions, job
  definitions, roles, log groups, state machines and an artifacts bucket (pass `bucket=` to use
  your own). Images are CDK Docker assets built from sfnkit's build contexts.
- **Names** are `<stack>-<construct id>-<name>`, so several workflows can share a stack.
- **Queues.** `queues=` binds each named `BatchQueue("...")` to a CDK job queue, an `Ec2Queue` or an
  ARN. `Ec2Queue` is a shortcut for the EC2 case: Spot or on-demand, a gp3 scratch disk of
  `scratch_gib` on every instance, and the GPU AMI with G5 instances for `gpu=True`.
- **Triggers.** `state_machine` works wherever CDK takes a state machine: EventBridge rules, Pipes
  from SQS, API Gateway. `add_schedule` uses EventBridge Scheduler and checks `input` against the
  workflow's arguments. `SCHEDULED_TIME` is the time the run was scheduled for, not when it
  started, so a late run or a backfill processes the right interval.
- **The artifacts bucket outlives the stack**, as CDK buckets do by default. Pass
  `removal_policy=cdk.RemovalPolicy.DESTROY, auto_delete_objects=True` to have it removed too.
- **Images Lambda accepts.** Docker 29 adds provenance attestations by default, which Lambda rejects.
  The assets are built with `attestation-inline=false` so they stay plain single-platform images.

`examples/cohort_cdk.py` is a full app, and `tests/test_e2e_cdk.py` deploys it with `cdk deploy`.

## Running steps on AWS Batch

Give a step `compute=Batch(...)` to run it as an AWS Batch job instead of a Lambda function. That's for
work that needs more than Lambda's 15 minutes or 10 GB, or a lot of parallelism.

```python
from sfnkit import Batch, BatchQueue, FargateQueue, step, workflow

spot = FargateQueue(subnets=["subnet-0abc..."], security_groups=["sg-0abc..."], spot=True, max_vcpus=256)
gpu = BatchQueue("arn:aws:batch:us-west-2:123456789012:job-queue/gpu", platform="EC2")  # made elsewhere

@step(compute=Batch(queue=spot, vcpus=4, memory=16384, timeout=4 * 3600))
def align(sample: dict, reference: str) -> str:
    ...  # returns the s3:// URI of the aligned file

@step(compute=Batch(queue=gpu, vcpus=8, memory=61440, gpus=1))
def call_variants(bam: str) -> str: ...

@workflow
def cohort(samples: list):
    bams = align.map(samples, reference="s3://refs/GRCh38.fa")  # one array job, a child per sample
    return call_variants.map(bams)  # a second array job over the first one's results
```

- **Queues.** `FargateQueue` creates a Fargate or Fargate Spot compute environment and job queue in
  the stack, and steps with equal queues share it. `BatchQueue(arn, platform=...)` uses a queue made
  elsewhere, such as CDK or Terraform. That's the way to get EC2 instances, GPUs or custom launch
  templates. `BatchQueue("gpu", platform="EC2")`, with a name instead of an ARN, keeps account
  details out of the code: the CDK construct or `sfnkit deploy --queue gpu=<arn>` binds it.
- **`step.map(items)` on a Batch step is one array job**, not a Map state: one submit, up to 10,000
  children. Each child reads its item from S3 and writes its result back.
  - `Batch(items_per_job=k)` gives each child k items, for lists over 10,000 or very small items.
  - An empty list skips the job.
  - Items can come from a step's output, another array job's results, a workflow input or a literal
    list.
  - `length(results)` works in conditions. Anything else about the results needs a step.
- **Inputs of any size.** The state machine writes each job's input to the artifacts bucket and the
  job gets its location, since a Batch submit takes at most 30 KiB and Fargate passes overrides on to
  ECS, which takes 8 KiB.
- **Results and errors work as with Lambda.** The job reports back through its task token, so
  `attempt`, `Retry(on=[...])` and `Err(...)` see the exception's class name. A failing array child
  fails the step with its error and terminates the rest of the array.
- **Two retry layers.** Batch retries infrastructure failures (image pull errors, Spot reclaims,
  lost hosts) up to `retries` attempts. Your own exceptions, running out of memory and the attempt
  timeout are never retried by Batch. Exceptions go to Step Functions instead.
- **`timeout` bounds each attempt's run time.** Time spent waiting in the queue isn't limited.
- **STANDARD workflows only.** Step Functions can't wait for Batch jobs in an EXPRESS workflow or an
  EXPRESS Distributed Map child, and the compiler rejects that.
- **Per-sample pipelines.** Inside `with each(samples, distributed=True) as it:`, Batch steps run as
  one job each, so every sample moves on without waiting for the slowest one.
- **Images.** Batch steps are built like Lambda steps, on the Lambda Python base image with the same
  `deps` and `system_packages`, but start `python -m sfnkit.runtime.container`. For tools that need
  another base (BioContainers, CUDA, conda), see [Custom base images](#custom-base-images).

`examples/batch.py` exercises all of this, and `tests/test_e2e_batch.py` runs it on Fargate Spot.

## Adding a compute backend

Every step runs through one interface, `sfnkit.compute.base.ComputeBackend`. To add one:

1. Subclass `ComputeSpec` for the user-facing settings, e.g. `Fargate(cpu=..., memory=...)`.
2. Implement four methods on the backend:
   - `resources(fn, ctx)`: the CloudFormation resources that run it.
   - `baseline_statements(fn, ctx)`: IAM its own role needs.
   - `task_state(fn)`: the Task state. Put `PAYLOAD` where the envelope goes.
   - `state_machine_statements(fn)`: IAM the state machine needs to start it.
3. Set the optional class attributes that apply:
   - `image_runtime = "container"` builds the image with `python -m sfnkit.runtime.container` as its
     entrypoint. That runtime reads the envelope and task token from the environment and reports
     with `SendTaskSuccess`/`SendTaskFailure`. Put `PAYLOAD_JSON` where the envelope goes as a string.
   - `standard_only = True` when Step Functions can only wait for the service in STANDARD workflows.
   - `supports_array = True` plus `array_task_state(fn, size)` makes `step.map` one native array job.
   - `stage_envelope = True` has the state machine write the envelope to S3 first and pass an S3
     wrapper in its place, for services with small request limits.
   - `parameters(fn)` declares template parameters the resources need, bound at deploy time (Batch
     uses it for named queues).
4. Register it with `register_backend` or the `sfnkit.compute` entry point.

The compiler, CloudFormation assembly and deployer only call this interface. `sfnkit.compute.batch`
is a complete example. `tests/core/test_compile.py` registers a fake ECS backend to prove nothing else
needs to change.

## Development

Work happens in the isb sandbox described by `isb.yaml`:

```
isb up -d
scripts/sandbox.sh -- 'uv sync && uv run pytest'                    # unit tests, no AWS
scripts/sandbox.sh -- 'uv run pytest -m docker'                       # build custom-base images for real
scripts/sandbox.sh -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  -- 'uv run pytest -m aws'                                            # validate ASL with AWS
scripts/sandbox.sh -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  -- 'PYTHONPATH=. uv run pytest -m e2e -s'                            # deploy, run, destroy
```

- **Golden ASL** lives in `tests/core/golden/`. Refresh it with `SFNKIT_UPDATE_GOLDEN=1` and
  review the diff.
- **The e2e tests** deploy under random `sfnkit-e2e-*` stack names, run every path, and always tear
  down. `test_e2e.py` deploys `examples/pipeline.py`; `test_e2e_batch.py` (`SFNKIT_SUBNETS`,
  `SFNKIT_SECURITY_GROUPS`) runs `examples/batch.py` on Fargate Spot; `test_e2e_cdk.py` (plus
  `SFNKIT_VPC`) deploys `examples/cohort_cdk.py` with the `cdk` CLI, using an asset bucket and
  repository it creates and deletes itself.
- **The Docker and CDK tests** need Docker and Node.js (for `aws-cdk-lib` and the `cdk` CLI) in
  the sandbox.
