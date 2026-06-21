from __future__ import annotations

from learnlake.normalization import parse_event_payload


def test_parse_event_payload_handles_json_string() -> None:
    payload = parse_event_payload('{"problem_id": "prob1"}')

    assert payload.kind == "json_string"
    assert payload.data == {"problem_id": "prob1"}


def test_parse_event_payload_handles_dict() -> None:
    payload = parse_event_payload({"problem_id": "prob1"})

    assert payload.kind == "json_dict"
    assert payload.data == {"problem_id": "prob1"}


def test_parse_event_payload_handles_list() -> None:
    payload = parse_event_payload([{"input": "choice_1"}])

    assert payload.kind == "list"
    assert payload.data == [{"input": "choice_1"}]


def test_parse_event_payload_handles_form_encoded_string() -> None:
    payload = parse_event_payload("input_1=choice_1&attempt=2")

    assert payload.kind == "form_encoded"
    assert payload.data == {"input_1": ["choice_1"], "attempt": ["2"]}


def test_parse_event_payload_marks_invalid_string() -> None:
    payload = parse_event_payload("not-json-and-not-form")

    assert payload.kind == "invalid_string"
    assert payload.data == "not-json-and-not-form"

