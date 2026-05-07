from apps.bronze_ingestor.config import BronzeConfig
from apps.bronze_ingestor.job import run


def main() -> None:
    run(BronzeConfig.from_env())


if __name__ == "__main__":
    main()
