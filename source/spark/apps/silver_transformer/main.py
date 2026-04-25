from apps.silver_transformer.config import SilverConfig
from apps.silver_transformer.job import run


def main() -> None:
    run(SilverConfig())


if __name__ == "__main__":
    main()
