from serving.trino.bootstrap.bootstrap_trino import (
    REGISTERED_TABLES,
    TRINO_BOOTSTRAP_TABLE_LOCATION_SCHEME,
)


def test_registered_tables_use_s3a_locations() -> None:
    assert REGISTERED_TABLES, "Expected at least one registered table"
    prefix = f"{TRINO_BOOTSTRAP_TABLE_LOCATION_SCHEME}://"
    assert all(table.location.startswith(prefix) for table in REGISTERED_TABLES)


def test_registered_table_names_are_unique() -> None:
    names = [table.name for table in REGISTERED_TABLES]
    assert len(names) == len(set(names))
