from domain.silver.dedup.technical import technical_dedup


def test_technical_dedup_symbol_exists() -> None:
    assert callable(technical_dedup)
