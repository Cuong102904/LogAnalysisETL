from projects.daotao_ai.gold.aggregation_config import GoldConfig
from projects.daotao_ai.gold.pipelines.batch_pipeline import run


def main() -> None:
    run(GoldConfig.from_env())


if __name__ == "__main__":
    main()
