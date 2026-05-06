from apps.bronze_ingestor.config import BronzeConfig
from domain.bronze.enricher import enrich_bronze
from domain.bronze.parser import parse_status_columns
from infrastructure.kafka.reader import read_kafka_stream
from infrastructure.spark.session import build_spark
from infrastructure.storage.delta import write_delta_stream


def run(config: BronzeConfig) -> None:
    spark = build_spark(config.app_name)
    raw = read_kafka_stream(
        spark,
        config.bootstrap_servers,
        config.topic,
        config.starting_offsets,
        config.consumer_group_id,
        config.consumer_client_id,
    )
    bronze = enrich_bronze(raw)
    bronze = parse_status_columns(bronze)
    write_delta_stream(
        bronze,
        output_path=config.output_path,
        checkpoint_path=config.checkpoint_path,
        partition_by=["ingest_date", "ingest_hour"],
    )
