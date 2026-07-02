from __future__ import annotations

from learnlake.runtime import load_silver_parser_config, load_silver_routing_config
from learnlake.runtime.config import resolve_path
from learnlake.silver.compiler import compile_routes


def test_daotao_routing_schema_compiles() -> None:
    routing = load_silver_routing_config(resolve_path("projects/daotao_ai/routing.yaml"))
    compiled = compile_routes(routing)

    assert routing.version == "2026-07-02"
    assert compiled[0].route.priority >= compiled[-1].route.priority
    assert any(route.route.parser == "parse_problem_check_browser" for route in compiled)


def test_daotao_parser_schema_loads() -> None:
    parsers = load_silver_parser_config(resolve_path("projects/daotao_ai/parsers.yaml"))

    assert parsers.version == "2026-07-02"
    assert any(parser.name == "parse_video_interaction" for parser in parsers.parsers)
    assert all(parser.inputs.required for parser in parsers.parsers)
