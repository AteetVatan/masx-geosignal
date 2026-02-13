"""Best-effort JSON parsing for LLM output."""

from __future__ import annotations

from typing import Any

import json_repair
import orjson
import pyjson5
import rapidjson


def loads_best_effort(raw: str) -> Any:
    """Best-effort parsing for JSON + malformed JSON from LLMs.

    Order:
      1) strict+fast (orjson)
      2) tolerant (rapidjson: comments + trailing commas)
      3) JSON5 (pyjson5: single quotes, unquoted keys, etc.)
      4) repair (json_repair: fixes broken LLM JSON)
    """
    s = raw.strip()

    # 1) Strict + fast
    try:
        return orjson.loads(s.encode("utf-8"))
    except Exception:
        pass

    # 2) Tolerant: comments + trailing commas
    try:
        return rapidjson.loads(
            s,
            parse_mode=rapidjson.PM_COMMENTS | rapidjson.PM_TRAILING_COMMAS,
        )
    except Exception:
        pass

    # 3) JSON5
    try:
        return pyjson5.loads(s)
    except Exception:
        pass

    # 4) LLM repair (can be used alone too)
    return json_repair.loads(s)
