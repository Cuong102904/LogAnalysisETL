from pipelines.gold.aggregation_pipeline import run

from apps.gold_aggregator.config import GoldConfig


def main() -> None:
    run(GoldConfig.from_env())


if __name__ == "__main__":
    main()
