from apps.bronze_ingestor.config import BronzeConfig


def test_bronze_config_loads_standard_shape(tmp_path, monkeypatch) -> None:
    config_file = tmp_path / "bronze_ingestor.yaml"
    config_file.write_text(
        "\n".join(
            [
                "app_name: bronze_ingestor",
                "input:",
                "  bootstrap_servers: broker1:29092,broker2:29092,broker3:29092",
                "  topic: mooc.raw.events",
                "  starting_offsets: latest",
                "storage:",
                "  table_path: s3a://lakehouse/mooc/bronze/mooc_events_raw",
                "checkpoint:",
                "  base_path: s3a://platform/mooc/bronze_ingestor",
                "streaming:",
                "  query_name: bronze_ingestor_raw",
                "options:",
                "  partition_by:",
                "    - ingest_date",
                "    - ingest_hour",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BRONZE_CONFIG_PATH", str(config_file))

    config = BronzeConfig.from_env()

    assert config.app_name == "bronze_ingestor"
    assert config.bootstrap_servers == "broker1:29092,broker2:29092,broker3:29092"
    assert config.topic == "mooc.raw.events"
    assert config.starting_offsets == "latest"
    assert config.output_path == "s3a://lakehouse/mooc/bronze/mooc_events_raw"
    assert config.checkpoint_path == "s3a://platform/mooc/bronze_ingestor"
    assert config.query_name == "bronze_ingestor_raw"
    assert config.partition_by == ("ingest_date", "ingest_hour")
