from pipelines.bronze.ingest_pipeline import run

from apps.bronze_ingestor.config import BronzeConfig


def main() -> None:
    run(BronzeConfig.from_env())


if __name__ == "__main__":
    main()
