# Gold Aggregator

Materialize gold analytics and anomaly signal tables from silver event streams.

Implemented tables:

- Analytics
  - `gold.video_friction_signals`
  - `gold.pdf_engagement_features`
  - `gold.quiz_attempt_metrics`
  - `gold.user_learning_profile_daily`
- Anomaly signals
  - `gold.exam_integrity_signals`
  - `gold.behavior_anomaly_signals`

Trigger interval is configurable and defaults to a near real-time micro-batch cadence.

Entry:

```bash
spark-submit --master spark://spark-master:7077 /opt/project/spark/apps/gold_aggregator/main.py
```
