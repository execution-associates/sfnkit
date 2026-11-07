"""A hand-built App (no tracer) for CloudFormation and deployer tests."""

from pathlib import Path
from types import SimpleNamespace

import infra_dummy_steps as steps

from sfnkit.app import App, AppSettings, FunctionSpec, ImageVariant, SmNeed, StateMachineSpec
from sfnkit.compute.lambda_ import Lambda
from sfnkit.iam import Allow, Policy

PROJECT = Path(__file__).parent


def make_app(project_dir: Path = PROJECT, exclude=()) -> App:
    img_a = ImageVariant("aaaa1111", ("pyyaml>=6",), ("git",), "3.14", "x86_64")
    img_b = ImageVariant("bbbb2222", (), (), "3.14", "arm64")
    base = Policy(Allow("s3:GetObject", "arn:aws:s3:::data/*"))
    fn_add = FunctionSpec("FnAdd1a2b3c4d", steps.add, Lambda(memory=1024, timeout=120), base, img_a)
    fn_add_more = FunctionSpec(
        "FnAdd5e6f7a8b", steps.add, Lambda(), base + Allow("dynamodb:PutItem", "*"), img_a, env={"MODE": "x"}
    )
    fn_echo = FunctionSpec("FnEcho9c0d1e2f", steps.echo, Lambda(), Policy(), img_b)

    child_def = {
        "QueryLanguage": "JSONata",
        "StartAt": "Echo",
        "States": {
            "Echo": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Arguments": {"FunctionName": "${FnEcho9c0d1e2fArn}", "Payload": "{% $states.input %}"},
                "Output": "{% $states.result.Payload %}",
                "End": True,
            }
        },
    }
    root_def = {
        "QueryLanguage": "JSONata",
        "StartAt": "Add",
        "States": {
            "Add": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Arguments": {"FunctionName": "${FnAdd1a2b3c4dArn}", "Payload": "{% $states.input %}"},
                "Assign": {"n1": "{% $states.result.Payload %}"},
                "Next": "Child",
            },
            "Child": {
                "Type": "Task",
                "Resource": "arn:aws:states:::states:startExecution.sync:2",
                "Arguments": {"StateMachineArn": "${SmScoringArn}", "Input": "{% {'x': $n1} %}"},
                "End": True,
            },
        },
    }
    root = StateMachineSpec(
        logical_id="SmMain",
        name="main",
        definition=root_def,
        substitutions={
            "FnAdd1a2b3c4dArn": {"Fn::GetAtt": ["FnAdd1a2b3c4d", "Arn"]},
            "SmScoringArn": {
                "Fn::Sub": "arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:stateMachine:"
                "${AWS::StackName}-scoring"
            },
        },
        type="STANDARD",
        needs=[
            SmNeed("invoke", fn=fn_add),
            SmNeed("invoke", fn=fn_add_more),
            SmNeed("start_child", child="scoring"),
            SmNeed("map_run"),
            SmNeed("bucket"),
            SmNeed("s3_read", bucket="user-data", prefix="in/"),
        ],
        root=True,
    )
    child = StateMachineSpec(
        logical_id="SmScoring",
        name="scoring",
        definition=child_def,
        substitutions={"FnEcho9c0d1e2fArn": {"Fn::GetAtt": ["FnEcho9c0d1e2f", "Arn"]}},
        type="EXPRESS",
        needs=[SmNeed("invoke", fn=fn_echo)],
    )
    return App(
        name="pipeline",
        module="infra_dummy_steps",
        workflow=SimpleNamespace(name="pipeline"),
        functions=[fn_add, fn_add_more, fn_echo],
        state_machines=[root, child],
        images=[img_a, img_b],
        settings=AppSettings(),
        project_dir=project_dir,
        exclude=tuple(exclude),
    )
