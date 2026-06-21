from domain.silver.classifier import add_classification


def test_classifier_symbol_exists() -> None:
    assert callable(add_classification)
