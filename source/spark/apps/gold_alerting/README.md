# Gold Alerting App

Streaming app that reads `gold.behavior_anomaly_signals` and materializes `gold.anomaly_alerts`.

## Output

- `s3a://lakehouse/mooc/gold/anomaly_alerts`

## Runtime

```bash
spark-submit --master spark://spark-master:7077 /opt/project/spark/apps/gold_alerting/main.py
```
