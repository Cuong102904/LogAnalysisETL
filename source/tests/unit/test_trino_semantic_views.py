from serving.trino.bootstrap.bootstrap_trino import validate_semantic_views


def test_semantic_view_sql_contracts_match_registry() -> None:
    validate_semantic_views()
