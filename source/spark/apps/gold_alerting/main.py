from apps.gold_alerting.config import GoldAlertingConfig
from pipelines.gold.alert_pipeline import run


def main() -> None:
    run(GoldAlertingConfig.from_env())


if __name__ == "__main__":
    main()
