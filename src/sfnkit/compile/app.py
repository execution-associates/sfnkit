"""Assemble an ``App`` from a root workflow: functions, images and every state machine it needs."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from sfnkit.app import App, AppSettings, FunctionSpec, ImageVariant, SmNeed, StateMachineSpec
from sfnkit.compile.asl import Compiler, Machine, render_fixpoint
from sfnkit.compute.base import backend_for
from sfnkit.compute.lambda_ import Lambda
from sfnkit.errors import CompileError
from sfnkit.ir import Graph, SubN, TaskN


def _h(*parts) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()


def _camel(s: str) -> str:
    return "".join(p[:1].upper() + p[1:] for p in re.split(r"[^A-Za-z0-9]+", s) if p) or "X"


def _sm_name(s: str) -> str:
    return (re.sub(r"[^A-Za-z0-9-]+", "-", s).strip("-") or "child")[:24]


def build_app(workflow, project_dir: Path | None = None) -> App:
    if workflow.module == "__main__":
        raise CompileError(
            f"workflow {workflow.name} is defined in __main__; put it in an importable module and refer to "
            "it as module:attr so the deployed runtime can import it"
        )
    return _Builder(workflow, Path(project_dir or Path.cwd())).build()


class _Builder:
    def __init__(self, root, project_dir: Path):
        self.root = root
        self.project_dir = project_dir
        self.settings = AppSettings(
            offload_threshold=root.offload_threshold, python=root.python, architecture=root.architecture
        )
        self.functions: dict[tuple, FunctionSpec] = {}
        self.images: dict[str, ImageVariant] = {}
        self.owner: dict[int, object] = {}
        self.children: dict[int, tuple[str, object]] = {}
        self.pending: list = []
        self.sub_names: dict[int, str] = {}
        self.used_names: set[str] = {"main"}
        self.compiler = Compiler(self.function_for, self.child_machine_for, self.sub_name_for)

    def unique(self, name: str) -> str:
        base, n = name, 2
        while name in self.used_names:
            suffix = f"-{n}"
            name = base[: 24 - len(suffix)] + suffix
            n += 1
        self.used_names.add(name)
        return name

    def note_owner(self, g: Graph) -> None:
        for t in g.tasks():
            self.owner[id(t)] = g.workflow

    def child_machine_for(self, wf):
        if id(wf) not in self.children:
            if wf is self.root:
                raise CompileError(f"workflow {wf.name} calls itself")
            self.children[id(wf)] = (self.unique(_sm_name(wf.name)), wf)
            self.pending.append(wf)
        return self.children[id(wf)]

    def sub_name_for(self, s: SubN) -> str:
        if id(s) not in self.sub_names:
            self.sub_names[id(s)] = self.unique(_sm_name(s.name))
        return self.sub_names[id(s)]

    def image_for(self, deps: list[str], system: list[str], runtime: str = "lambda", img=None) -> ImageVariant:
        parts: list = [deps, system, self.settings.python, self.settings.architecture]
        if runtime != "lambda":  # Lambda keys stay as they were before runtimes existed
            parts.append(runtime)
        custom = img is not None and img.custom
        if custom:  # likewise for images without a custom base
            parts.append([img.base, list(img.conda), list(img.channels), img.python])
        key = _h(*parts)[:12]
        if key not in self.images:
            self.images[key] = ImageVariant(
                key=key,
                deps=tuple(deps),
                system_packages=tuple(system),
                python=self.settings.python,
                architecture=self.settings.architecture,
                runtime=runtime,
                base=img.base if custom else None,
                conda=tuple(img.conda) if custom else (),
                channels=tuple(img.channels) if custom and img.conda else (),
                python_mode=img.python if custom else None,
            )
        return self.images[key]

    def function_for(self, t: TaskN) -> FunctionSpec:
        owner = self.owner.get(id(t), self.root)
        step = t.step
        compute = step.compute or getattr(owner, "default_compute", None) or self.root.default_compute or Lambda()
        # The step's image, else its own workflow's, else the root's; deps and packages always add up.
        img = step.image or getattr(owner, "image", None) or self.root.image
        deps = sorted(
            set(self.root.deps) | set(getattr(owner, "deps", ())) | set(step.deps) | set(img.deps if img else ())
        )
        system = sorted(
            set(self.root.system_packages)
            | set(getattr(owner, "system_packages", ()))
            | set(step.system_packages)
            | set(img.system_packages if img else ())
        )
        image = self.image_for(deps, system, backend_for(compute).image_runtime, img)
        key = (step.key, t.policy.digest(), repr(compute), image.key)
        fn = self.functions.get(key)
        if fn is None:
            lid = f"Fn{_camel(step.name)[:40]}{_h(*key)[:6]}"
            fn = FunctionSpec(logical_id=lid, step=step, compute=compute, policy=t.policy, image=image,
                              env=dict(step.env))
            self.functions[key] = fn
        return fn

    def build(self) -> App:
        c = self.compiler
        g = self.root.graph()
        self.note_owner(g)
        root_m = Machine(name="main", kind="root", type=self.root.type)
        c.machines.append(root_m)
        c.compile_machine(root_m, g, "root")
        compiled: set[int] = set()
        while self.pending:
            wf = self.pending.pop(0)
            if id(wf) in compiled:
                continue
            compiled.add(id(wf))
            cg = wf.graph()
            self.note_owner(cg)
            name, _ = self.children[id(wf)]
            m = Machine(name=name, kind="call", type=wf.type)
            c.machines.append(m)
            c.compile_machine(m, cg, "call")
        defs = render_fixpoint([m.definition for m in c.machines])
        specs = []
        for m, d in zip(c.machines, defs):
            needs = [SmNeed("invoke", fn=fn) for fn in m.functions]
            needs += [SmNeed("start_child", child=ch) for ch in m.children]
            if m.map_run:
                needs.append(SmNeed("map_run"))
            if "ArtifactsBucket" in m.substitutions:
                needs.append(SmNeed("bucket"))
            for bucket, prefix in m.s3_reads:
                needs.append(SmNeed("s3_read", bucket=bucket, prefix=prefix))
            specs.append(
                StateMachineSpec(
                    logical_id=f"Sm{_camel(m.name)}",
                    name=m.name,
                    definition=d,
                    substitutions=dict(m.substitutions),
                    type=m.type,  # type: ignore[arg-type]
                    needs=needs,
                    root=m is root_m,
                )
            )
        express_with_waits = [s.name for s, m in zip(specs, c.machines) if m.type == "EXPRESS" and m.map_run]
        if express_with_waits:
            raise CompileError(f"EXPRESS workflows can't run a Distributed Map: {', '.join(express_with_waits)}")
        return App(
            name=_sm_name(self.root.name).lower(),
            module=self.root.module,
            workflow=self.root,
            functions=list(self.functions.values()),
            state_machines=specs,
            images=list(self.images.values()),
            settings=self.settings,
            project_dir=self.project_dir,
            exclude=tuple(self.root.exclude),
        )
