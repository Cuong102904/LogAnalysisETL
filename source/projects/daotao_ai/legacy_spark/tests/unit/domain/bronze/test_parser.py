from shared.json_utils import parse_json_or_none


def test_parse_json_or_none_ok() -> None:
    assert parse_json_or_none('{"a": 1}') == {"a": 1}


def test_parse_json_or_none_fail() -> None:
    assert parse_json_or_none("{") is None
