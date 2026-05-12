from dataclasses import dataclass

from apps.config_utils import env_str, load_app_config, nested_value, required_env_str


@dataclass(frozen=True)
class BronzeConfig:
    app_name: str
    bootstrap_servers: str
    topic: str
    starting_offsets: str
    output_path: str
    checkpoint_path: str
    query_name: str
    partition_by: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "BronzeConfig":
        config = load_app_config("BRONZE_CONFIG_PATH", "bronze_ingestor.yaml")
        return cls(
            app_name=env_str("BRONZE_APP_NAME", config, "app_name", default="bronze_ingestor"),
            bootstrap_servers=required_env_str(
                "KAFKA_BOOTSTRAP_SERVERS",
                config,
                "input",
                "bootstrap_servers",
            ),
            topic=required_env_str("BRONZE_INPUT_TOPIC", config, "input", "topic"),
            starting_offsets=required_env_str(
                "BRONZE_STARTING_OFFSETS",
                config,
                "input",
                "starting_offsets",
            ),
            output_path=required_env_str(
                "BRONZE_TABLE_PATH",
                config,
                "storage",
                "table_path",
            ),
            checkpoint_path=required_env_str(
                "BRONZE_CHECKPOINT_PATH",
                config,
                "checkpoint",
                "base_path",
            ),
            query_name=env_str(
                "BRONZE_QUERY_NAME",
                config,
                "streaming",
                "query_name",
                default="bronze_ingestor_raw",
            ),
            partition_by=tuple(
                str(value)
                for value in nested_value(
                    config,
                    "options",
                    "partition_by",
                    default=("ingest_date", "ingest_hour"),
                )
            ),
        )
