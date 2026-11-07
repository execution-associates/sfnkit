"""IAM policies for steps.

Three layers end up on a step's role:
  1. the runtime baseline (logs + the artifacts bucket), added automatically;
  2. the workflow's base set, ``@workflow(iam=base)``, inherited by steps with no ``iam=``;
  3. a step's own ``iam=``, which replaces the base set. Write ``base + Allow(...)`` to extend it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any


class Statement:
    """One IAM statement. ``Allow(...)`` / ``Deny(...)`` build these."""

    __slots__ = ("_key", "actions", "condition", "effect", "resources")

    def __init__(self, effect: str, actions, resources="*", condition: dict | None = None):
        self.effect = effect
        self.actions = tuple([actions] if isinstance(actions, str) else actions)
        self.resources = tuple([resources] if isinstance(resources, (str, dict)) else resources)
        self.condition = condition
        if not self.actions:
            raise ValueError("an IAM statement needs at least one action")
        self._key = json.dumps(self.to_json(), sort_keys=True, default=str)

    def to_json(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "Effect": self.effect,
            "Action": list(self.actions),
            "Resource": list(self.resources),
        }
        if self.condition:
            doc["Condition"] = self.condition
        return doc

    def __add__(self, other) -> Policy:
        return Policy(self) + other

    def __eq__(self, other):
        return isinstance(other, Statement) and other._key == self._key

    def __hash__(self):
        return hash(self._key)

    def __repr__(self):
        return f"{self.effect}({list(self.actions)!r}, {list(self.resources)!r})"


def Allow(actions: str | Iterable[str], resources: Any = "*", condition: dict | None = None) -> Statement:
    return Statement("Allow", actions, resources, condition)


def Deny(actions: str | Iterable[str], resources: Any = "*", condition: dict | None = None) -> Statement:
    return Statement("Deny", actions, resources, condition)


class Policy:
    """An ordered, de-duplicated set of statements. ``+`` takes the union."""

    __slots__ = ("statements",)

    def __init__(self, *statements: Statement | Policy):
        out: list[Statement] = []
        for s in statements:
            for st in s.statements if isinstance(s, Policy) else (s,):
                if not isinstance(st, Statement):
                    raise TypeError(f"Policy takes Allow(...)/Deny(...) statements, got {st!r}")
                if st not in out:
                    out.append(st)
        self.statements: tuple[Statement, ...] = tuple(out)

    def __add__(self, other: Statement | Policy) -> Policy:
        if not isinstance(other, (Statement, Policy)):
            return NotImplemented
        return Policy(self, other)

    __radd__ = __add__

    def __bool__(self):
        return bool(self.statements)

    def __eq__(self, other):
        return isinstance(other, Policy) and self.digest() == other.digest()

    def __hash__(self):
        return hash(self.digest())

    def to_statements(self) -> list[dict[str, Any]]:
        return [s.to_json() for s in self.statements]

    def digest(self) -> str:
        """Short stable hash, used to give one function per (step, effective policy)."""
        raw = json.dumps([s._key for s in self.statements])
        return hashlib.sha256(raw.encode()).hexdigest()[:8]

    def __repr__(self):
        return f"Policy({', '.join(map(repr, self.statements))})"


EMPTY = Policy()
