import json

import pytest
from botocore.exceptions import ClientError, WaiterError
from infra_app import make_app

from sfnkit.deploy import client
from sfnkit.deploy.deployer import deploy, deploy_stack, destroy, validate_stack_name
from sfnkit.errors import DeployError


def _missing(op="DescribeStacks"):
    return ClientError(
        {"Error": {"Code": "ValidationError", "Message": "Stack with id x does not exist"}}, op
    )


class FakeWaiter:
    def __init__(self, cfn, name):
        self.cfn, self.name = cfn, name

    def wait(self, **kw):
        self.cfn.waited.append(self.name)
        if self.name in self.cfn.fail_waiters:
            raise WaiterError(self.name, "failed", {})
        if self.name == "stack_delete_complete":
            self.cfn.stacks.pop(kw["StackName"], None)


class FakeCfn:
    """Just enough CloudFormation to drive the deployer's control flow."""

    def __init__(self, stacks=None, fail_waiters=(), no_changes=False):
        self.stacks = dict(stacks or {})  # name -> {"status": ..., "outputs": {...}}
        self.fail_waiters = set(fail_waiters)
        self.no_changes = no_changes
        self.change_sets, self.executed, self.deleted_cs, self.waited, self.deleted = [], [], [], [], []
        self.outputs_for = {}

    def describe_stacks(self, StackName):
        if StackName not in self.stacks:
            raise _missing()
        s = self.stacks[StackName]
        outs = [{"OutputKey": k, "OutputValue": v} for k, v in s.get("outputs", {}).items()]
        return {"Stacks": [{"StackStatus": s["status"], "Outputs": outs}]}

    def create_change_set(self, **kw):
        self.change_sets.append(kw)

    def describe_change_set(self, StackName, ChangeSetName):
        reason = "The submitted information didn't contain changes." if self.no_changes else "bad template"
        return {"Status": "FAILED", "StatusReason": reason}

    def delete_change_set(self, **kw):
        self.deleted_cs.append(kw)

    def execute_change_set(self, StackName, ChangeSetName):
        self.executed.append(StackName)
        self.stacks[StackName] = {"status": "CREATE_COMPLETE", "outputs": self.outputs_for.get(StackName, {})}

    def delete_stack(self, StackName):
        self.deleted.append(StackName)

    def describe_stack_events(self, StackName):
        return {
            "StackEvents": [
                {
                    "LogicalResourceId": "FnX",
                    "ResourceStatus": "CREATE_FAILED",
                    "ResourceStatusReason": "no image",
                },
                {"LogicalResourceId": "Ok", "ResourceStatus": "CREATE_COMPLETE"},
            ]
        }

    def get_waiter(self, name):
        if name == "change_set_create_complete" and (self.no_changes or name in self.fail_waiters):
            return _Fail()
        return FakeWaiter(self, name)


class _Fail:
    def wait(self, **kw):
        raise WaiterError("change_set_create_complete", "failed", {})


def test_validate_stack_name():
    app = make_app()
    validate_stack_name("sfnkit-pipeline", app)
    with pytest.raises(DeployError):
        validate_stack_name("1bad", app)
    with pytest.raises(DeployError):
        validate_stack_name("has_underscore", app)
    with pytest.raises(DeployError, match="80"):
        validate_stack_name("s" * 75, app)  # + "-scoring" is over 80


def test_create_path():
    cfn = FakeCfn()
    cfn.outputs_for["s"] = {"A": "1"}
    assert deploy_stack(cfn, "s", template_body="{}", log=lambda *a: None) == {"A": "1"}
    assert cfn.change_sets[0]["ChangeSetType"] == "CREATE"
    assert "stack_create_complete" in cfn.waited


def test_no_changes_is_success():
    cfn = FakeCfn(stacks={"s": {"status": "UPDATE_COMPLETE", "outputs": {"B": "2"}}}, no_changes=True)
    assert deploy_stack(cfn, "s", template_body="{}", log=lambda *a: None) == {"B": "2"}
    assert cfn.change_sets[0]["ChangeSetType"] == "UPDATE" and cfn.deleted_cs and not cfn.executed


def test_failed_change_set_raises():
    cfn = FakeCfn(fail_waiters={"change_set_create_complete"})
    with pytest.raises(DeployError, match="bad template"):
        deploy_stack(cfn, "s", template_body="{}", log=lambda *a: None)


def test_failed_create_reports_resource_reasons():
    cfn = FakeCfn(fail_waiters={"stack_create_complete"})
    with pytest.raises(DeployError, match="FnX: no image"):
        deploy_stack(cfn, "s", template_body="{}", log=lambda *a: None)


def test_rollback_complete_is_deleted_then_recreated():
    cfn = FakeCfn(stacks={"s": {"status": "ROLLBACK_COMPLETE"}})
    deploy_stack(cfn, "s", template_body="{}", log=lambda *a: None)
    assert cfn.deleted == ["s"] and cfn.change_sets[0]["ChangeSetType"] == "CREATE"


def test_busy_stack_raises():
    cfn = FakeCfn(stacks={"s": {"status": "UPDATE_IN_PROGRESS"}})
    with pytest.raises(DeployError, match="busy"):
        deploy_stack(cfn, "s", template_body="{}", log=lambda *a: None)


class FakeS3:
    def __init__(self):
        self.put, self.deleted = [], []

    def put_object(self, **kw):
        self.put.append(kw)

    def get_paginator(self, name):
        class P:
            def paginate(self, Bucket):
                return [{"Versions": [{"Key": "runs/a", "VersionId": "null"}], "DeleteMarkers": []}]

        return P()

    def delete_objects(self, Bucket, Delete):
        self.deleted.append((Bucket, Delete))


class FakeSession:
    region_name = "us-west-2"

    def __init__(self, cfn):
        self.cfn, self.s3, self.ecr = cfn, FakeS3(), object()

    def client(self, name):
        return {"cloudformation": self.cfn, "s3": self.s3, "ecr": self.ecr}[name]


class FakeBuilder:
    def __init__(self):
        self.built = []

    def ensure_image(self, context, repository_uri, ecr):
        self.built.append(context.image.key)
        return f"{repository_uri}:{context.tag}"


def test_deploy_orchestration(tmp_path, monkeypatch):
    app = make_app(project_dir=tmp_path)
    monkeypatch.setattr("sfnkit.build.context.import_roots", lambda app, image: [tmp_path])
    cfn = FakeCfn()
    cfn.outputs_for["sfnkit-pipeline-assets"] = {"BucketName": "art", "RepositoryUri": "1.dkr/repo"}
    cfn.outputs_for["sfnkit-pipeline"] = {"StateMachineArn": "arn:sm"}
    builder = FakeBuilder()
    res = deploy(app, session=FakeSession(cfn), builder=builder, log=lambda *a: None)
    assert (res.stack_name, res.state_machine_arn, res.bucket) == ("sfnkit-pipeline", "arn:sm", "art")
    assert builder.built == ["aaaa1111", "bbbb2222"]
    main_cs = cfn.change_sets[1]
    params = {p["ParameterKey"]: p["ParameterValue"] for p in main_cs["Parameters"]}
    assert params["ArtifactsBucket"] == "art" and params["Imageaaaa1111Uri"].startswith("1.dkr/repo:")
    assert main_cs["Capabilities"] == ["CAPABILITY_IAM"]
    assert json.loads(main_cs["TemplateBody"])["Outputs"]["StateMachineArn"]


def test_destroy_empties_bucket_and_is_idempotent():
    cfn = FakeCfn(
        stacks={
            "sfnkit-pipeline": {"status": "CREATE_COMPLETE"},
            "sfnkit-pipeline-assets": {"status": "CREATE_COMPLETE", "outputs": {"BucketName": "art"}},
        }
    )
    session = FakeSession(cfn)
    destroy("sfnkit-pipeline", session=session, log=lambda *a: None)
    assert cfn.deleted == ["sfnkit-pipeline", "sfnkit-pipeline-assets"]
    assert session.s3.deleted[0][0] == "art"
    destroy("sfnkit-pipeline", session=session, log=lambda *a: None)  # nothing left: no error
    assert cfn.deleted == ["sfnkit-pipeline", "sfnkit-pipeline-assets"]


# ---------------------------------------------------------------------------------------------- client


class FakeSfn:
    def __init__(self, descs):
        self.descs = list(descs)

    def start_execution(self, **kw):
        self.started = kw
        return {"executionArn": "arn:ex"}

    def describe_execution(self, executionArn):
        return self.descs.pop(0)


class ClientSession:
    def __init__(self, sfn, s3=None):
        self.sfn, self.s3 = sfn, s3

    def client(self, name):
        return {"stepfunctions": self.sfn, "s3": self.s3}[name]


def test_client_run_resolves_output():
    out = json.dumps({"$sw": 1, "v": {"receipt": {"$sw": 1, "x": {"$sw": 1, "v": 7}}}})
    sfn = FakeSfn([{"status": "RUNNING"}, {"status": "SUCCEEDED", "output": out}])
    assert client.run("arn:sm", {"a": 1}, poll=0, session=ClientSession(sfn)) == {"receipt": 7}
    assert json.loads(sfn.started["input"]) == {"a": 1}


def test_client_run_raises_on_failure():
    cause = json.dumps({"errorMessage": "nope", "errorType": "LowScore"})
    sfn = FakeSfn([{"status": "FAILED", "error": "LowScore", "cause": cause, "executionArn": "arn:ex"}])
    with pytest.raises(client.ExecutionFailed) as ei:
        client.run("arn:sm", {}, poll=0, session=ClientSession(sfn))
    assert ei.value.error == "LowScore" and ei.value.cause["errorMessage"] == "nope"
