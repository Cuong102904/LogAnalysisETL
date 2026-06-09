from pipelines.gold.alert_pipeline import run

from apps.gold_alerting.config import GoldAlertingConfig


def main() -> None:
    run(GoldAlertingConfig.from_env())


if __name__ == "__main__":
    main()
