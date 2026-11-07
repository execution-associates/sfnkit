"""Building and resolving the wrappers described in ``sfnkit.runtime.protocol``.

``wrap`` runs inside a step runtime on the step's return value. ``resolve`` runs on the way in (step
arguments) and on the client (execution output), and turns every wrapper back into plain values.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

from sfnkit.runtime import protocol as P

DEFAULT_THRESHOLD = 32 * 1024
# Step Functions allows 256 KiB per state result; keep headroom for Assign/metadata around it.
MAX_RESULT_BYTES = 250_000

_MISSING = object()
_client = None


def _s3(s3=None):
    global _client
    if s3 is not None:
        return s3
    if _client is None:
        import boto3

        _client = boto3.client("s3")
    return _client


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// uri: {uri!r}")
    bucket, _, key = uri[5:].partition("/")
    return bucket, key


def _get_json(bucket: str, key: str, s3=None) -> Any:
    body = _s3(s3).get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def _put_json(bucket: str, key: str, value: Any, s3=None) -> None:
    _s3(s3).put_object(Bucket=bucket, Key=key, Body=_dumps(value).encode(), ContentType="application/json")


def get_path(value: Any, path) -> Any:
    """Follow ``path`` into ``value``; ``_MISSING`` if any step doesn't exist."""
    cur = value
    for p in path:
        if (
            isinstance(cur, dict)
            and isinstance(p, str)
            and p in cur
            or (
                isinstance(cur, list)
                and isinstance(p, int)
                and not isinstance(p, bool)
                and -len(cur) <= p < len(cur)
            )
        ):
            cur = cur[p]
        else:
            return _MISSING
    return cur


def _is_wrapper(value: Any) -> bool:
    return isinstance(value, dict) and value.get(P.MARK) == P.VERSION


# ---------------------------------------------------------------------------------------------- resolve


def resolve(value: Any, s3=None) -> Any:
    """Turn any wrapper (and wrappers nested in lists/dicts) into the plain value it stands for."""
    if isinstance(value, list):
        return [resolve(v, s3) for v in value]
    if not isinstance(value, dict):
        return value
    if not _is_wrapper(value):
        return {k: resolve(v, s3) for k, v in value.items()}
    if P.VALUE in value:
        return resolve(value[P.VALUE], s3)
    if P.S3 in value:
        return resolve(_get_json(*parse_uri(value[P.S3]), s3=s3), s3)
    if P.CARRIED in value:
        return resolve(value[P.CARRIED], s3)
    if P.PROJ in value:
        base = resolve(value[P.PROJ], s3)
        path = value.get(P.PROJ_PATH, [])
        out = get_path(base, path)
        # Same as JSONata and the local runner: a missing key or index reads as None.
        return None if out is _MISSING else out
    if P.CAUSE in value:
        return _decode_cause(value[P.CAUSE])
    if P.DMAP in value:
        return _read_dmap(value[P.DMAP], s3)
    raise ValueError(f"unknown sfnkit wrapper: {sorted(value)}")


def _decode_cause(cause: Any) -> Any:
    if not isinstance(cause, str):
        return cause
    try:
        return json.loads(cause)
    except ValueError:
        return cause


def _read_dmap(details: dict, s3=None) -> list:
    """Read a Distributed Map ResultWriter manifest back into an ordered list of results."""
    bucket = details["Bucket"]
    manifest = _get_json(bucket, details["Key"], s3=s3)
    dest = manifest.get("DestinationBucket") or bucket
    files = manifest.get("ResultFiles", {})
    by_index: dict[int, Any] = {}
    unindexed: list[Any] = []

    for entry in files.get("SUCCEEDED", []):
        for child in _get_json(dest, entry["Key"], s3=s3):
            out = json.loads(child["Output"]) if isinstance(child.get("Output"), str) else child.get("Output")
            idx = out.get(P.INDEX) if _is_wrapper(out) else None
            if idx is None:
                idx = _input_index(child)
            if idx is None:
                unindexed.append(resolve(out, s3))
            else:
                by_index[int(idx)] = resolve(out, s3)

    # Tolerated failures keep their slot so results stay aligned with the input items.
    for entry in files.get("FAILED", []):
        for child in _get_json(dest, entry["Key"], s3=s3):
            idx = _input_index(child)
            if idx is not None:
                by_index.setdefault(int(idx), None)

    if not by_index:
        return unindexed
    out = [None] * (max(by_index) + 1)
    for i, v in by_index.items():
        out[i] = v
    return out + unindexed


def _input_index(child: dict) -> int | None:
    raw = child.get("Input")
    try:
        inp = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return None
    if isinstance(inp, dict) and isinstance(inp.get("index"), int):
        return inp["index"]
    return None


# ------------------------------------------------------------------------------------------------- wrap


class _Offloader:
    def __init__(self, execution: str, node: str, bucket: str | None, threshold: int, s3):
        self.execution, self.node, self.bucket, self.threshold, self.s3 = (
            execution,
            node,
            bucket,
            threshold,
            s3,
        )

    def key(self) -> str:
        return f"{P.RUNS_PREFIX}{self.execution}/{self.node}/{uuid.uuid4().hex}.json"

    def put(self, value: Any) -> tuple[str, str]:
        if not self.bucket:
            raise RuntimeError(f"value too large to pass inline and no {P.ENV_BUCKET} is set for offloading")
        key = self.key()
        _put_json(self.bucket, key, value, self.s3)
        return self.bucket, key

    def wrap_value(self, value: Any) -> dict:
        if len(_dumps(value)) > self.threshold:
            bucket, key = self.put(value)
            return {P.MARK: P.VERSION, P.S3: f"s3://{bucket}/{key}"}
        return {P.MARK: P.VERSION, P.VALUE: value}


_FACT_OPS = {
    # A path may end in an operator so a condition like is_null(x) doesn't copy all of x inline.
    "$null": lambda v: v is None,
    "$bool": lambda v: bool(v),
    "$len": lambda v: len(v) if isinstance(v, (list, dict, str)) else 0,
}


def _facts(value: Any, facts: dict | None) -> dict:
    out = {}
    for k, path in (facts or {}).items():
        path = list(path)
        op = _FACT_OPS.get(path[-1]) if path and isinstance(path[-1], str) else None
        if op is not None:
            path = path[:-1]
        v = get_path(value, path)
        v = None if v is _MISSING else v
        out[k] = op(v) if op is not None else v
    return out


def wrap(
    value: Any,
    *,
    execution: str,
    node: str,
    bucket: str | None,
    threshold: int = DEFAULT_THRESHOLD,
    facts: dict | None = None,
    items: dict | None = None,
    s3=None,
) -> dict:
    """Build the result wrapper for a step's return value (offloading to S3 as needed)."""
    try:
        _dumps(value)
    except (TypeError, ValueError) as e:
        raise TypeError(f"step result isn't JSON-serializable: {e}") from None
    off = _Offloader(execution, node, bucket, threshold, s3)
    out = off.wrap_value(value)
    if facts:
        out[P.FACTS] = _facts(value, facts)
    if items:
        facets = {}
        for key, spec in items.items():
            arr = get_path(value, spec.get("path", []))
            if arr is _MISSING or not isinstance(arr, list):
                raise TypeError(
                    f"expected a list at {spec.get('path', [])!r} of the step result to map over, "
                    f"got {type(arr).__name__ if arr is not _MISSING else 'nothing'}"
                )
            elements = []
            for elem in arr:
                w = off.wrap_value(elem)
                if spec.get("facts"):
                    w[P.FACTS] = _facts(elem, spec["facts"])
                elements.append(w)
            if spec.get("mode", "inline") == "s3":
                b, k = off.put(elements)
                facets[key] = {"bucket": b, "key": k}
            else:
                facets[key] = elements
        out[P.ITEMS] = facets
    size = len(_dumps(out))
    if size > MAX_RESULT_BYTES:
        raise ValueError(
            f"step result wrapper is {size} bytes, over the {MAX_RESULT_BYTES}-byte state limit even after "
            "offloading values; for long lists use .map(..., distributed=True) or each(..., distributed=True)"
        )
    return out


def threshold_from_env() -> int:
    raw = os.environ.get(P.ENV_THRESHOLD)
    return int(raw) if raw else DEFAULT_THRESHOLD
