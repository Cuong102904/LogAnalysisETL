from __future__ import annotations

from projects.daotao_ai.gold.exam_ops_config import GoldExamOpsConfig
from projects.daotao_ai.gold.pipelines.exam_ops_stream_pipeline import run


def main() -> None:
    run(GoldExamOpsConfig.from_env())


if __name__ == "__main__":
    main()
