from apps.gold_aggregator.config import GoldConfig
from apps.gold_aggregator.job import run


def main() -> None:
    run(GoldConfig())


if __name__ == "__main__":
    main()
