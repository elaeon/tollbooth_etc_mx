import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import pytest

from tb_map_editor.utils.query_parser import parse_query


def test_simple_values():
    assert parse_query("id:1,2") == {"param": "id", "values": ["1", "2"]}


def test_whitespace_and_name():
    assert parse_query(" name : Main Toll ") == {"param": "name", "values": ["Main Toll"]}


def test_h3_cell_valid():
    res = parse_query("h3_cell:8928308280,8")
    assert res["param"] == "h3_cell"
    assert res["cell"] == 8928308280
    assert res["resolution"] == 8


def test_h3_cell_missing_resolution():
    with pytest.raises(ValueError):
        parse_query("h3_cell:8928308280")


def test_no_colon():
    with pytest.raises(ValueError):
        parse_query("no_colon_string")


def test_all_value():
    assert parse_query("id:all") == {"param": "id", "values": ["all"]}
