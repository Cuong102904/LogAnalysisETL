from __future__ import annotations

from projects.daotao_ai.gold.alerting_config import GoldAlertingConfig
from projects.daotao_ai.gold.pipelines.alert_pipeline import run


def main() -> None:
    run(GoldAlertingConfig.from_env())


if __name__ == "__main__":
    main()
