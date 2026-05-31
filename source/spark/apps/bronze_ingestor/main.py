from apps.bronze_ingestor.config import BronzeConfig
from pipelines.bronze.ingest_pipeline import run


def main() -> None:
    run(BronzeConfig.from_env())


if __name__ == "__main__":
    main()
