"""Inspect a completed `RunResult` to see whether the model invoked tools (RPC-backed `FunctionTool`s)."""

from __future__ import annotations

from typing import Any

from agents import RunResult


def tool_call_names_from_result(result: RunResult) -> list[str]:
    """Return the `name` of each function tool the model requested (e.g. `pull_and_run_image`).

    Walks `result.new_items` and collects `ToolCallItem` entries from the Responses API payload.
    """

    names: list[str] = []
    for item in result.new_items:
        if getattr(item, "type", None) != "tool_call_item":
            continue
        raw = item.raw_item
        name = _tool_name_from_raw(raw)
        if name:
            names.append(name)
    return names


def tool_call_details_from_result(result: RunResult) -> list[dict[str, Any]]:
    """Return one dict per tool call: name, raw arguments string, and optional call_id."""

    details: list[dict[str, Any]] = []
    for item in result.new_items:
        if getattr(item, "type", None) != "tool_call_item":
            continue
        raw = item.raw_item
        name = _tool_name_from_raw(raw)
        arguments = _tool_arguments_from_raw(raw)
        call_id = _tool_call_id_from_raw(raw)
        row: dict[str, Any] = {"name": name, "arguments": arguments}
        if call_id:
            row["call_id"] = call_id
        details.append(row)
    return details


def _tool_name_from_raw(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        n = raw.get("name")
        return str(n) if n else None
    name = getattr(raw, "name", None)
    return str(name) if name else None


def _tool_arguments_from_raw(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        a = raw.get("arguments")
        return str(a) if a is not None else None
    a = getattr(raw, "arguments", None)
    return str(a) if a is not None else None


def _tool_call_id_from_raw(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        cid = raw.get("call_id") or raw.get("id")
        return str(cid) if cid else None
    cid = getattr(raw, "call_id", None) or getattr(raw, "id", None)
    return str(cid) if cid else None
