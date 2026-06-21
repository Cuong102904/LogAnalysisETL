from delta.tables import DeltaTable
from learnlake.runtime import build_spark
from projects.daotao_ai.gold.alerting_config import GoldAlertingConfig
from projects.daotao_ai.gold.domain.alerts import build_alert_events
from projects.daotao_ai.gold.schemas.behavior_anomalies import BEHAVIOR_ANOMALY_SIGNALS_SCHEMA


def _ensure_behavior_anomaly_signals_schema(spark, path: str) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return

    empty_df = spark.createDataFrame([], BEHAVIOR_ANOMALY_SIGNALS_SCHEMA)
    (empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))


def run(config: GoldAlertingConfig) -> None:
    spark = build_spark(config.app_name)
    _ensure_behavior_anomaly_signals_schema(spark, config.input_behavior_anomaly_signals_path)
    behavior_anomaly_signals = spark.readStream.format("delta").load(
        config.input_behavior_anomaly_signals_path
    )
    alert_events = build_alert_events(
        behavior_anomaly_signals,
        min_event_count=config.min_event_count,
        min_distinct_users=config.min_distinct_users,
        min_z_score=config.min_z_score,
    )
    alert_events = alert_events.withWatermark("alert_time", "7 days").dropDuplicates(["alert_id"])

    (
        alert_events.writeStream.format("delta")
        .outputMode("append")
        .option("path", config.output_anomaly_alerts_path)
        .option("checkpointLocation", f"{config.checkpoint_base}/anomaly_alerts")
        .option("mergeSchema", "true")
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .queryName(config.query_name)
        .partitionBy("event_date", "alert_severity")
        .start()
    )

    spark.streams.awaitAnyTermination()
