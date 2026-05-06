from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeConfig:
    app_name: str = "bronze_ingestor"
    bootstrap_servers: str = "broker1:29092,broker2:29092,broker3:29092"
    topic: str = "mooc.raw.events"
    starting_offsets: str = "latest"
    consumer_group_id: str = "bronze_ingestor"
    consumer_client_id: str = "bronze_ingestor_worker"
    output_path: str = "s3a://bronze/mooc/bronze/mooc_events_raw"
    checkpoint_path: str = "s3a://checkpoints/mooc/bronze_ingestor"
