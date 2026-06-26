from delta.tables import DeltaTable
from learnlake.runtime import build_spark
from projects.daotao_ai.gold.alerting_config import GoldAlertingConfig
from projects.daotao_ai.gold.domain.alerts import build_alert_events
from projects.daotao_ai.gold.schemas.behavior_anomalies import BEHAVIOR_ANOMALY_SIGNALS_SCHEMA
from projects.daotao_ai.gold.pipelines.runtime import run_periodic_job, write_batch_output


def _ensure_behavior_anomaly_signals_schema(spark, path: str) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return

    empty_df = spark.createDataFrame([], BEHAVIOR_ANOMALY_SIGNALS_SCHEMA)
    (empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))


def run(config: GoldAlertingConfig) -> None:
    spark = build_spark(config.app_name)
    _ensure_behavior_anomaly_signals_schema(spark, config.input_behavior_anomaly_signals_path)
    interval_seconds = int(config.trigger_interval_seconds)

    def cycle() -> None:
        behavior_anomaly_signals = spark.read.format("delta").load(
            config.input_behavior_anomaly_signals_path
        )
        alert_events = build_alert_events(
            behavior_anomaly_signals,
            min_event_count=config.min_event_count,
            min_distinct_users=config.min_distinct_users,
            min_z_score=config.min_z_score,
        ).dropDuplicates(["alert_id"])
        write_batch_output(
            alert_events,
            config.output_anomaly_alerts_path,
            ("event_date", "alert_severity"),
        )

    run_periodic_job(config.query_name, interval_seconds, cycle)
