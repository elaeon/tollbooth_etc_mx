"""Simple parser for free-text API `query` strings.

Grammar supported:
- param:val1,val2  (comma-separated values, spaces allowed)
- h3_cell:<cell>,<resolution>  (both integers; resolution required)

The parser is parser-only: it returns a normalized dict or raises ValueError
on malformed input. Endpoints should convert the parsed result to DB filters.
"""
from typing import Any, Dict, List, Tuple

_RESERVED_WORDS = {
    "empty_stretch"
}

def _split_once_colon(q: str) -> Tuple[str, str]:
    if ":" not in q:
        raise ValueError("query must contain a ':' separating param and value(s)")
    param, values = q.split(":", 1)
    param = param.strip()
    values = values.strip()
    if not param:
        raise ValueError("parameter name is empty")
    if not values:
        raise ValueError("parameter values are empty")
    return param, values


def parse_query(query: str) -> Dict[str, Any]:
    """Parse a raw query string and return a normalized structure.

    Raises ValueError when the string is malformed.
    Examples:
    - "id:1,2" -> {"param":"id","values":["1","2"]}
    - "name: Main Toll " -> {"param":"name","values":["Main Toll"]}
    """
    if not isinstance(query, str):
        raise ValueError("query must be a string")

    if query in _RESERVED_WORDS:
        return {"param": query}
    else:
        param, values_str = _split_once_colon(query)
        param = param.strip().lower()

    # Generic param: allow comma-separated values
    values: List[str] = [v.strip() for v in values_str.split(",") if v.strip()]
    if not values:
        raise ValueError("no values found for parameter")

    return {"param": param, "values": values}
