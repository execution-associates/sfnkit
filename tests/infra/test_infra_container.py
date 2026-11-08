import json

import infra_dummy_steps  # noqa: F401

from sfnkit.runtime.container import main


class FakeSfn:
    def __init__(self):
        self.calls = []

    def send_task_success(self, **kw):
        self.calls.append(("success", kw))

    def send_task_failure(self, **kw):
        self.calls.append(("failure", kw))


def _env(step, args):
    envelope = {"sw": 1, "step": f"infra_dummy_steps.{step}", "execution": "e", "node": "n", "args": args}
    return {"SFNKIT_ENVELOPE": json.dumps(envelope), "SFNKIT_TASK_TOKEN": "tok"}


def test_success_sends_wrapper():
    sfn = FakeSfn()
    assert main(environ=_env("add", {"a": 1, "b": 2}), sfn=sfn) == 0
    kind, kw = sfn.calls[0]
    assert kind == "success" and kw["taskToken"] == "tok"
    assert json.loads(kw["output"]) == {"$sw": 1, "v": 3}


def test_failure_reports_class_name_and_lambda_shaped_cause():
    sfn = FakeSfn()
    assert main(environ=_env("decline", {"order": "o"}), sfn=sfn) == 1
    kind, kw = sfn.calls[0]
    assert kind == "failure" and kw["error"] == "PaymentDeclined"
    cause = json.loads(kw["cause"])
    assert cause["errorType"] == "PaymentDeclined" and "declined" in cause["errorMessage"]


def test_envelope_from_s3(s3):
    envelope = {"sw": 1, "step": "infra_dummy_steps.add", "execution": "e", "node": "n", "args": {"a": 5}}
    s3.put_object(Bucket="art", Key="env.json", Body=json.dumps(envelope))
    sfn = FakeSfn()
    environ = {
        "SFNKIT_ENVELOPE": json.dumps({"$sw": 1, "s3": "s3://art/env.json"}),
        "SFNKIT_TASK_TOKEN": "t",
    }
    assert main(environ=environ, sfn=sfn, s3=s3) == 0
    assert json.loads(sfn.calls[0][1]["output"]) == {"$sw": 1, "v": 6}


def test_staged_envelope_keeps_offloaded_args_for_execute(s3):
    s3.put_object(Bucket="art", Key="runs/e/n/big.json", Body=json.dumps(40))
    envelope = {"sw": 1, "step": "infra_dummy_steps.add", "execution": "e", "node": "n",
                "args": {"a": {"$sw": 1, "s3": "s3://art/runs/e/n/big.json"}, "b": 2}}
    s3.put_object(Bucket="art", Key="runs/e/n/in/x.json", Body=json.dumps(envelope))
    sfn = FakeSfn()
    environ = {"SFNKIT_ENVELOPE": json.dumps({"$sw": 1, "s3": "s3://art/runs/e/n/in/x.json"}),
               "SFNKIT_TASK_TOKEN": "t"}
    assert main(environ=environ, sfn=sfn, s3=s3) == 0
    assert json.loads(sfn.calls[0][1]["output"]) == {"$sw": 1, "v": 42}
