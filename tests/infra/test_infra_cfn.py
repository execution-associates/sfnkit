import json

from infra_app import make_app

from sfnkit.compile.cfn import assets_template, main_template


def lint_errors(template: dict) -> list[str]:
    from cfnlint.api import ManualArgs, lint

    matches = lint(json.dumps(template), config=ManualArgs(regions=["us-west-2"]))
    return [f"{m.rule.id} {m.message}" for m in matches if m.rule.id.startswith("E")]


def statements(role: dict) -> list[dict]:
    pols = role["Properties"].get("Policies", [])
    return [s for p in pols for s in p["PolicyDocument"]["Statement"]]


def test_assets_template_lints_clean():
    tpl = assets_template(make_app())
    assert lint_errors(tpl) == []
    repo = tpl["Resources"]["Repository"]["Properties"]
    assert repo["EmptyOnDelete"] is True
    rules = tpl["Resources"]["Bucket"]["Properties"]["LifecycleConfiguration"]["Rules"]
    assert {"Id": "expire-runs", "Status": "Enabled", "Prefix": "runs/", "ExpirationInDays": 30} in rules
    assert set(tpl["Outputs"]) == {"RepositoryUri", "RepositoryName", "BucketName"}


def test_main_template_lints_clean():
    assert lint_errors(main_template(make_app())) == []


def test_parameters_functions_and_roles():
    app = make_app()
    tpl = main_template(app)
    assert set(tpl["Parameters"]) == {"ArtifactsBucket", "Imageaaaa1111Uri", "Imagebbbb2222Uri"}
    res = tpl["Resources"]

    fn = res["FnAdd1a2b3c4d"]["Properties"]
    assert fn["PackageType"] == "Image"
    assert fn["Code"] == {"ImageUri": {"Ref": "Imageaaaa1111Uri"}}
    assert fn["MemorySize"] == 1024 and fn["Timeout"] == 120
    assert fn["Environment"]["Variables"]["SFNKIT_MODULE"] == "infra_dummy_steps"
    assert fn["Environment"]["Variables"]["SFNKIT_BUCKET"] == {"Ref": "ArtifactsBucket"}
    assert res["FnEcho9c0d1e2f"]["Properties"]["Architectures"] == ["arm64"]
    assert res["FnAdd5e6f7a8b"]["Properties"]["Environment"]["Variables"]["MODE"] == "x"
    assert res["FnAdd1a2b3c4dLogs"]["Type"] == "AWS::Logs::LogGroup"

    role = res["FnAdd1a2b3c4dRole"]
    assert role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "lambda.amazonaws.com"
    }
    acts = [s["Action"] for s in statements(role)]
    assert ["s3:GetObject"] in acts  # the user's base set
    assert ["dynamodb:PutItem"] not in acts
    assert ["dynamodb:PutItem"] in [s["Action"] for s in statements(res["FnAdd5e6f7a8bRole"])]
    # An own-only empty policy still gets the runtime baseline, nothing from the base set.
    echo_acts = [s["Action"] for s in statements(res["FnEcho9c0d1e2fRole"])]
    assert ["s3:GetObject"] not in echo_acts and ["s3:GetObject", "s3:PutObject"] in echo_acts


def test_state_machines_and_their_roles():
    tpl = main_template(make_app())
    res = tpl["Resources"]
    sm = res["SmMain"]
    assert sm["Type"] == "AWS::StepFunctions::StateMachine"
    props = sm["Properties"]
    assert props["StateMachineName"] == {"Fn::Sub": "${AWS::StackName}-main"}
    assert props["StateMachineType"] == "STANDARD"
    assert "SmScoringArn" in props["DefinitionSubstitutions"]
    assert sm["DependsOn"] == ["SmScoring"]
    assert res["SmScoring"]["Properties"]["StateMachineType"] == "EXPRESS"

    st = statements(res["SmMainRole"])
    actions = {a for s in st for a in ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])}
    assert {
        "lambda:InvokeFunction",
        "states:StartExecution",
        "states:DescribeExecution",
        "events:PutRule",
        "states:DescribeMapRun",
        "s3:PutObject",
        "s3:ListBucket",
    } <= actions
    user = [s for s in st if s["Action"] == "s3:GetObject"]
    assert user and "user-data/in/*" in user[0]["Resource"]["Fn::Sub"]
    assert tpl["Outputs"]["StateMachineArn"] == {"Value": {"Ref": "SmMain"}}


def test_everything_taggable_is_tagged():
    tpl = main_template(make_app())
    for lid, r in tpl["Resources"].items():
        tags = r["Properties"].get("Tags")
        assert tags is not None, lid
        assert {"Key": "sfnkit:workflow", "Value": "pipeline"} in tags
        assert {"Key": "sfnkit:managed", "Value": "true"} in tags


def test_lint_helper_catches_errors():
    # Guards against the lint helper silently passing everything.
    bad = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {"B": {"Type": "AWS::S3::Bucket", "Properties": {"Nope": 1}}},
    }
    assert lint_errors(bad)
