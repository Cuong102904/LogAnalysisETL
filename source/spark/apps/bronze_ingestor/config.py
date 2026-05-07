from dataclasses import dataclass

from apps.config_utils import env_str, load_app_config, required_env_str


@dataclass(frozen=True)
class BronzeConfig:
    app_name: str
    bootstrap_servers: str
    topic: str
    starting_offsets: str
    consumer_group_id: str
    consumer_client_id: str
    output_path: str
    checkpoint_path: str
    query_name: str

    @classmethod
    def from_env(cls) -> "BronzeConfig":
        config = load_app_config("BRONZE_CONFIG_PATH", "bronze_ingestor.yaml")
        return cls(
            app_name=env_str("BRONZE_APP_NAME", config, "app_name", default="bronze_ingestor"),
            bootstrap_servers=required_env_str(
                "KAFKA_BOOTSTRAP_SERVERS",
                config,
                "kafka",
                "bootstrap_servers",
            ),
            topic=required_env_str("KAFKA_TOPIC_RAW", config, "kafka", "input_topic"),
            starting_offsets=required_env_str(
                "BRONZE_STARTING_OFFSETS",
                config,
                "kafka",
                "starting_offsets",
            ),
            consumer_group_id=required_env_str(
                "BRONZE_CONSUMER_GROUP_ID",
                config,
                "kafka",
                "consumer_group_id",
            ),
            consumer_client_id=required_env_str(
                "BRONZE_CONSUMER_CLIENT_ID",
                config,
                "kafka",
                "consumer_client_id",
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
                "storage",
                "checkpoint_path",
            ),
            query_name=env_str(
                "BRONZE_QUERY_NAME",
                config,
                "streaming",
                "query_name",
                default="bronze_ingestor_raw",
            ),
        )
