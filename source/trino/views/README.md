# Trino Semantic Views

These SQL files define the semantic layer for behavior intelligence use cases.

They are intentionally kept as plain SQL so they can be:
- executed from Trino CLI
- pasted into Superset SQL Lab
- replayed during bootstrap scripts later

Expected source tables:
- `delta.mooc.silver_video_interactions`
- `delta.mooc.video_friction_signals`
- `delta.mooc.exam_integrity_signals`
- `delta.mooc.pdf_engagement_features`
- `delta.mooc.quiz_attempt_metrics`
- `delta.mooc.user_learning_profile_daily`
- `delta.mooc.behavior_anomaly_signals`
- `delta.mooc.anomaly_alerts`

Views:
- `video_friction_view`
- `exam_anomaly_view`
- `pdf_engagement_view`
- `quiz_difficulty_view`
- `course_improvement_view`
- `learner_health_view`
- `behavior_anomaly_view`
- `alert_events_view`

The repo does not auto-apply these statements yet. They are the canonical definitions for the semantic layer.

Superset can consume the same semantic layer directly through Trino. The dashboard bootstrap under `superset/bootstrap/` uses these view names as the source of truth for datasets and charts, so the BI layer stays code-driven instead of manually rebuilt in the UI.
