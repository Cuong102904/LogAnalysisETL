from apps.silver_transformer.config import SilverConfig
from pipelines.silver.transform_pipeline import run


def main() -> None:
    run(SilverConfig.from_env())


if __name__ == "__main__":
    main()
