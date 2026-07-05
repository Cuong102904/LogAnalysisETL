from __future__ import annotations

from projects.daotao_ai.gold.assessment_config import AssessmentStreamConfig
from projects.daotao_ai.gold.pipelines.assessment_stream_pipeline import run


def main() -> None:
    run(AssessmentStreamConfig.from_env())


if __name__ == "__main__":
    main()
