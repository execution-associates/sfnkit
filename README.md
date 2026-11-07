# sfnkit

Write AWS Step Functions workflows as plain Python. Steps are decorated functions that run as Lambda
container images. The workflow body is ordinary Python, including `match`, `if` and `with` blocks,
and is compiled into a state machine.

```
pip install sfnkit
```

Requires Python 3.12+. Deploying also needs Docker (to build the Lambda images) and AWS credentials
that can create IAM roles.

```python
from sfnkit import step, workflow, attempt, Ok, Err, each, subworkflow, wait, fail, is_null
from sfnkit import Lambda, Retry, Allow, Policy

base = Policy(Allow("s3:GetObject", "arn:aws:s3:::data/*"))

@step(compute=Lambda(memory=1024, timeout=120), deps=["httpx>=0.27"])
def fetch(url: str) -> list[str]: ...

@step(deps=["pyyaml"], retry=Retry(on=[ValueError], max_attempts=3))
def parse(doc: str) -> dict: ...

@workflow(iam=base)
def pipeline(url: str, label: str = "run"):
    docs = fetch(url)
    rows = parse.map(docs, max_concurrency=10)          # Map state

    with subworkflow("scoring"):                         # child state machine
        score = score_rows(rows)

    match score["tier"]:                                 # Choice state
        case "gold" | "silver":
            order = publish(rows)
        case "review" if score["value"] > 0.5:
            wait(seconds=300)
            order = queue_for_review(rows)
        case _:
            fail("LowScore", "score below 0.5")

    match attempt(charge, order):                        # Catch
        case Ok(receipt):
            pass
        case Err("PaymentDeclined", cause):
            receipt = refund(order, cause)

    return {"receipt": receipt}
```

```
sfnkit local  myapp.flows:pipeline --input '{"url": "https://..."}'
sfnkit synth  myapp.flows:pipeline -o out/
sfnkit deploy myapp.flows:pipeline --region us-west-2
sfnkit run    myapp.flows:pipeline --input '{"url": "https://..."}'
sfnkit destroy myapp.flows:pipeline
```

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
| `step.map(items, **kw)`, `with each(items) as it:` | Map (inline), or Distributed with `distributed=True` |
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

## Deploying

`deploy` creates two stacks:

- `<stack>-assets`: the ECR repository and the artifacts bucket.
- `<stack>`: the functions, their roles, log groups and state machines.

`destroy` deletes both, emptying the bucket first. The deploying credentials must be allowed to
create IAM roles. Credentials from `sts get-session-token` can't call IAM without MFA, so use
long-term or assumed-role credentials.

## Adding a compute backend

Every step runs through one interface, `sfnkit.compute.base.ComputeBackend`. To add one:

1. Subclass `ComputeSpec` for the user-facing settings, e.g. `Fargate(cpu=..., memory=...)`.
2. Implement four methods on the backend:
   - `resources(fn, ctx)`: the CloudFormation resources that run it.
   - `baseline_statements(fn, ctx)`: IAM its own role needs.
   - `task_state(fn)`: the Task state. Put `PAYLOAD` where the envelope goes.
   - `state_machine_statements(fn)`: IAM the state machine needs to start it.
3. Register it with `register_backend` or the `sfnkit.compute` entry point.

The compiler, CloudFormation assembly and deployer only call this interface. Containers can run the
same image with the entrypoint `python -m sfnkit.runtime.container`, which reads the envelope and
task token from the environment and reports the result with `SendTaskSuccess`/`SendTaskFailure`.
`tests/core/test_compile.py` registers a fake ECS backend to prove nothing else needs to change.

## Development

Work happens in the isb sandbox described by `isb.yaml`:

```
isb up -d
scripts/sandbox.sh -- 'uv sync && uv run pytest'                    # unit tests, no AWS
scripts/sandbox.sh -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  -- 'uv run pytest -m aws'                                            # validate ASL with AWS
scripts/sandbox.sh -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  -- 'PYTHONPATH=. uv run pytest -m e2e -s'                            # deploy, run, destroy
```

- **Golden ASL** lives in `tests/core/golden/`. Refresh it with `SFNKIT_UPDATE_GOLDEN=1` and
  review the diff.
- **The e2e test** deploys `examples/pipeline.py` under a random `sfnkit-e2e-*` stack name, runs
  every path, and always tears down.
