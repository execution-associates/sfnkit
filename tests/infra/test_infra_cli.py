import json
import sys

import pytest

from sfnkit.cli import build_parser, load_workflow, main, parse_input


def test_parse_subcommands():
    p = build_parser()
    a = p.parse_args(["deploy", "myapp.flows:pipeline", "--region", "us-west-2", "--stack-name", "s1"])
    assert (a.command, a.target, a.region, a.stack_name, a.profile) == (
        "deploy",
        "myapp.flows:pipeline",
        "us-west-2",
        "s1",
        None,
    )
    a = p.parse_args(["run", "m:wf", "--input", '{"x": 1}', "--no-wait"])
    assert a.no_wait and a.input == '{"x": 1}'
    a = p.parse_args(["synth", "m:wf", "-o", "out"])
    assert str(a.out) == "out"
    with pytest.raises(SystemExit):
        p.parse_args(["local"])  # target is required
    with pytest.raises(SystemExit):
        p.parse_args(["synth", "m:wf", "--region", "x"])  # synth takes no AWS options


def test_parse_input(tmp_path):
    assert parse_input(None) == {}
    assert parse_input('{"a": [1]}') == {"a": [1]}
    f = tmp_path / "in.json"
    f.write_text('{"b": 2}')
    assert parse_input(f"@{f}") == {"b": 2}
    with pytest.raises(SystemExit):
        parse_input("{not json")


@pytest.fixture
def wf_module(tmp_path):
    (tmp_path / "cli_flows_mod.py").write_text(
        "class WF:\n"
        "    name = 'demo'\n"
        "    def local(self, **inputs):\n"
        "        return {'got': inputs}\n"
        "wf = WF()\n"
    )
    yield tmp_path
    sys.modules.pop("cli_flows_mod", None)


def test_load_workflow(wf_module):
    wf = load_workflow("cli_flows_mod:wf", wf_module)
    assert wf.name == "demo"
    with pytest.raises(SystemExit):
        load_workflow("no_colon_here", wf_module)


def test_local_command(wf_module, capsys):
    rc = main(["local", "cli_flows_mod:wf", "--project-dir", str(wf_module), "--input", '{"x": 1}'])
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == {"got": {"x": 1}}
