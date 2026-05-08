import os
import sys

sys.path.append(os.path.dirname(os.path.realpath("src")))

import pytest

from src.utils.query_parser import parse_query


def test_simple_values():
    assert parse_query("id:1,2") == {"param": "id", "values": ["1", "2"]}


def test_whitespace_and_name():
    assert parse_query(" name : Main Toll ") == {"param": "name", "values": ["Main Toll"]}


def test_no_reserved_word():
    with pytest.raises(ValueError):
        parse_query("no_colon_string")


def test_all_value():
    assert parse_query("id:all") == {"param": "id", "values": ["all"]}


def test_reserved_words():
    assert parse_query("empty_stretch") == {"param": "empty_stretch"}

