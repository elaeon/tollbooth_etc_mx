"""Simple parser for free-text API `query` strings.

Grammar supported:
- param:val1,val2  (comma-separated values, spaces allowed)
- h3_cell:<cell>,<resolution>  (both integers; resolution required)

The parser is parser-only: it returns a normalized dict or raises ValueError
on malformed input. Endpoints should convert the parsed result to DB filters.
"""
from typing import Any, Dict, List, Tuple


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
    - "h3_cell:8928308280,8" -> {"param":"h3_cell","cell":8928308280,"resolution":8}
    """
    if not isinstance(query, str):
        raise ValueError("query must be a string")

    param, values_str = _split_once_colon(query)
    param = param.strip().lower()

    if param == "h3_cell":
        parts = [p.strip() for p in values_str.split(",") if p.strip()]
        if len(parts) != 2:
            raise ValueError("h3_cell queries must provide 'cell,resolution' (e.g. h3_cell:8928...,8)")
        try:
            cell = int(parts[0])
            resolution = int(parts[1])
        except ValueError:
            raise ValueError("h3_cell cell and resolution must be integers")
        return {"param": "h3_cell", "cell": cell, "resolution": resolution}

    # Generic param: allow comma-separated values
    values: List[str] = [v.strip() for v in values_str.split(",") if v.strip()]
    if not values:
        raise ValueError("no values found for parameter")

    return {"param": param, "values": values}
