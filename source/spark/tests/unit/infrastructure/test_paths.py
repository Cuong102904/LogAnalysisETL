from infrastructure.storage.paths import TablePaths


def test_table_paths_create() -> None:
    paths = TablePaths("a", "b", "c", "d", "e", "f", "g")
    assert paths.bronze_raw == "a"
