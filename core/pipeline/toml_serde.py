"""TOML serde for LLM interaction — token-efficient alternative to JSON.

Ported from AIGalileoArena's debate infrastructure.  TOML uses fewer tokens
than JSON (no braces, no key quoting) which reduces cost when sending
structured data to LLMs and parsing structured responses.

Flow:  dict → dict_to_toml() → prompt → LLM → toml_to_dict() → dict
"""

from __future__ import annotations

import re
import tomllib
from typing import Any

import tomli_w

_FENCE_RE = re.compile(r"```(?:toml)?\s*\n(.*?)```", re.DOTALL)


# ── Serialisation helpers ──────────────────────────────


def _strip_none(data: Any) -> Any:
    """TOML has no null — drop None values recursively."""
    if isinstance(data, dict):
        return {k: _strip_none(v) for k, v in data.items() if v is not None}
    if isinstance(data, list):
        return [_strip_none(item) for item in data]
    return data


def _ensure_floats(
    data: Any,
    float_keys: frozenset[str] = frozenset({"confidence", "score"}),
) -> Any:
    """Keep known float fields as float so TOML writes 0.9 not 0."""
    if isinstance(data, dict):
        out: dict[str, Any] = {}
        for k, v in data.items():
            if k in float_keys and isinstance(v, int):
                out[k] = float(v)
            else:
                out[k] = _ensure_floats(v, float_keys)
        return out
    if isinstance(data, list):
        return [_ensure_floats(item, float_keys) for item in data]
    return data


def dict_to_toml(data: dict[str, Any]) -> str:
    """Dict → TOML string (strips None, coerces known float fields)."""
    cleaned = _ensure_floats(_strip_none(data))
    return tomli_w.dumps(cleaned)


# ── Deserialisation helpers ────────────────────────────


def toml_to_dict(text: str) -> dict[str, Any]:
    """Parse TOML (with markdown-fence stripping).  Raises ValueError on failure."""
    cleaned = _extract_toml_block(text)
    try:
        return tomllib.loads(cleaned)
    except tomllib.TOMLDecodeError:
        pass

    try:
        return tomllib.loads(text.strip())
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"Could not parse TOML: {exc}") from exc


def _extract_toml_block(text: str) -> str:
    """Extract TOML content from text that may contain markdown fences."""
    stripped = text.strip()

    # fenced ```toml ... ```
    match = _FENCE_RE.search(stripped)
    if match:
        return match.group(1).strip()

    # generic ``` fences
    if stripped.startswith("```"):
        first_nl = stripped.find("\n")
        last_fence = stripped.rfind("```", first_nl)
        if first_nl != -1 and last_fence > first_nl:
            return stripped[first_nl + 1 : last_fence].strip()

    # already looks like TOML
    if _looks_like_toml(stripped):
        return stripped

    # find first TOML-ish line in mixed output
    for idx, line in enumerate(stripped.splitlines()):
        if _looks_like_toml(line):
            return "\n".join(stripped.splitlines()[idx:]).strip()

    return stripped


def _looks_like_toml(text: str) -> bool:
    first = text.lstrip()
    if not first:
        return False
    if first.startswith("["):
        return True
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=", first))
