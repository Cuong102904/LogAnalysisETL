from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from kafka.src.common.core_producer import build_dlq_payload, replay_stream
from kafka.src.models.replay_record import ReplayRecord
from kafka.src.common.actor_identity import event_has_subject_identity
from kafka.src.filters.mooc_event_filter import is_event_allowed, load_producer_filter_config


class MoocEventFilterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cfg_path = Path(__file__).resolve().parents[1] / "config" / "producer_filter.yaml"
        cls.cfg = load_producer_filter_config(cfg_path)

    def test_video_event_allowed(self) -> None:
        event = {"event_type": "play_video", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_sequence_event_allowed(self) -> None:
        event = {"event_type": "edx.ui.lms.sequence.next_selected", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_problem_event_allowed(self) -> None:
        event = {"event_type": "problem_check", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_proctoring_api_allowed(self) -> None:
        event = {"event_type": "/api/edx_proctoring/v1/proctored_exam/attempt", "event_source": "server"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_learning_content_pdf_allowed(self) -> None:
        event = {"event_type": "textbook.pdf.display.scaled", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_navigation_link_clicked_allowed(self) -> None:
        event = {"event_type": "edx.ui.lms.link_clicked", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_access_dashboard_allowed(self) -> None:
        event = {"event_type": "/dashboard", "event_source": "server"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_completion_event_allowed(self) -> None:
        event = {"event_type": "edx.video.completed", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_unallowed_event_filtered_out(self) -> None:
        event = {"event_type": "internal.vendor.opaque_ping", "event_source": "server"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertFalse(allowed, reason)
        self.assertIn("filtered_out", reason)

    def test_bi_marketing_event_allowed(self) -> None:
        event = {"event_type": "edx.bi.course.upgrade.sidebarupsell.displayed", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)
        self.assertTrue(reason.startswith("product_and_marketing"))

    def test_edx_residual_and_lms_urls_allowed(self) -> None:
        for et, prefix in (
            ("edx.cohort.membership.updated", "edx_taxonomy_residual"),
            ("/courses/course-v1:Org+X+Y/info", "leading_slash_path_residual"),
            ("/api/user/v1/accounts/anon", "leading_slash_path_residual"),
            ("/robots.txt", "leading_slash_path_residual"),
        ):
            with self.subTest(et=et):
                event = {"event_type": et, "event_source": "server"}
                allowed, reason = is_event_allowed(event, self.cfg)
                self.assertTrue(allowed, reason)
                self.assertTrue(reason.startswith(prefix))

    def test_server_goto_position_allowed(self) -> None:
        event = {
            "event_type": "/courses/course-v1:Org+CRS+RUN/xblock/block-v1:Org+CRS+RUN+type@sequential+block@abc/handler/xmodule_handler/goto_position",
            "event_source": "server",
        }
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)
        self.assertTrue(reason.startswith("sequence_navigation"))

    def test_server_problem_handler_urls_allowed(self) -> None:
        for et in (
            "/courses/x/xblock/+type@problem+block@b/handler/xmodule_handler/problem_check",
            "/courses/x/xblock/+type@problem+block@b/handler/xmodule_handler/input_ajax",
        ):
            with self.subTest(et=et):
                event = {"event_type": et, "event_source": "server"}
                allowed, reason = is_event_allowed(event, self.cfg)
                self.assertTrue(allowed, reason)
                self.assertTrue(reason.startswith("assessment_performance"))

    def test_video_save_user_state_url_allowed(self) -> None:
        event = {
            "event_type": "/courses/a/xblock/block-v1:A+run+type@video+block@id/handler/xmodule_handler/save_user_state",
            "event_source": "server",
        }
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)
        self.assertTrue(reason.startswith("video_engagement"))

    def test_transcript_handler_url_allowed(self) -> None:
        event = {
            "event_type": "/courses/a/xblock/block-v1:A+run+type@video+block@id/handler/transcript/translation/en",
            "event_source": "server",
        }
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)
        self.assertTrue(reason.startswith("video_engagement"))

    def test_problem_show_browser_allowed(self) -> None:
        event = {"event_type": "problem_show", "event_source": "browser"}
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_resume_course_engagement_allowed(self) -> None:
        event = {
            "event_type": "edx.course.home.resume_course.clicked",
            "event_source": "browser",
        }
        allowed, reason = is_event_allowed(event, self.cfg)
        self.assertTrue(allowed, reason)

    def test_event_type_contains_all_matcher_supported(self) -> None:
        from kafka.src.filters import mooc_event_filter as mf

        group = {"event_type_contains_all": ["aaa", "bbb"], "event_type_contains_any": []}
        matched = mf._group_match_reason(
            group_name="g",
            group_cfg=group,
            event_type="xxAAAXXbbbYY",
            event_source="browser",
            event_name="",
            context_path="/",
        )
        self.assertIsNotNone(matched)


class DlqPayloadShapeTest(unittest.TestCase):
    def test_build_dlq_payload_has_required_fields(self) -> None:
        payload = build_dlq_payload(
            ingest_time="2026-01-17T21:36:38.892173+00:00",
            error_type="filtered_out",
            error_message="reason",
            raw_event={"event_type": "x"},
            failed_topic="mooc.raw.events",
            failed_key="missing-key",
        )
        self.assertIn("ingest_time", payload)
        self.assertIn("error_type", payload)
        self.assertIn("error_message", payload)
        self.assertIn("raw_event", payload)


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def produce(self, topic, key=None, value=None, on_delivery=None) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout) -> None:  # pragma: no cover
        return None

    def flush(self, timeout) -> None:  # pragma: no cover
        return None


class CoreReplayRoutingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cfg_path = Path(__file__).resolve().parents[1] / "config" / "producer_filter.yaml"
        cls.cfg = load_producer_filter_config(cfg_path)

    def _record(self, event: dict, key: bytes = b"user:test") -> ReplayRecord:
        raw = json.dumps(event)
        return ReplayRecord(
            raw_line=raw,
            event=event,
            key_bytes=key,
            event_time=datetime.now(tz=timezone.utc),
            decode_error=None,
            validation_ok=True,
            validation_reason="",
        )

    def test_allowed_event_routes_to_raw_topic(self) -> None:
        producer = _FakeProducer()
        record = self._record({"event_type": "play_video", "event_source": "browser", "name": "play_video"})

        stats = replay_stream(
            records=[record],
            producer=producer,
            raw_topic="mooc.raw.events",
            dlq_topic="mooc.dlq.events",
            speed=9999.0,
            dlq_failed_topic="mooc.raw.events",
            filter_fn=is_event_allowed,
            filter_cfg=self.cfg,
        )

        self.assertEqual(stats["sent_raw"], 1)
        self.assertEqual(stats.get("sent_raw_anonymous", 0), 0)
        self.assertEqual(stats["sent_dlq"], 0)
        self.assertEqual(len(producer.calls), 1)
        self.assertEqual(producer.calls[0]["topic"], "mooc.raw.events")

    def test_filtered_event_routes_to_dlq_with_taxonomy(self) -> None:
        producer = _FakeProducer()
        event = {
            "event_type": "internal.vendor.telemetry_ping",
            "event_source": "server",
            "name": "",
            "context": {"path": "/"},
        }
        record = self._record(event)

        stats = replay_stream(
            records=[record],
            producer=producer,
            raw_topic="mooc.raw.events",
            dlq_topic="mooc.dlq.events",
            speed=9999.0,
            dlq_failed_topic="mooc.raw.events",
            filter_fn=is_event_allowed,
            filter_cfg=self.cfg,
        )

        self.assertEqual(stats["sent_raw"], 0)
        self.assertEqual(stats["sent_dlq"], 1)
        self.assertEqual(len(producer.calls), 1)
        self.assertEqual(producer.calls[0]["topic"], "mooc.dlq.events")

        payload = json.loads(producer.calls[0]["value"].decode("utf-8"))
        self.assertEqual(payload["error_type"], "filtered_out")
        self.assertIn("snapshot=", payload["error_message"])
        self.assertEqual(payload["failed_topic"], "mooc.raw.events")
        self.assertEqual(payload["failed_key"], "user:test")

    def test_allowlisted_without_identity_routes_anonymous_topic(self) -> None:
        producer = _FakeProducer()
        event = {
            "event_type": "play_video",
            "event_source": "browser",
            "name": "play_video",
            "time": "2026-01-01T12:00:00+00:00",
            "context": {},
        }
        stats = replay_stream(
            records=[self._record(event, key=b"session:xyz")],
            producer=producer,
            raw_topic="mooc.raw.events",
            dlq_topic="mooc.dlq.events",
            speed=9999.0,
            dlq_failed_topic="mooc.raw.events",
            filter_fn=is_event_allowed,
            filter_cfg=self.cfg,
            anonymous_topic="mooc.raw.anonymous.events",
        )
        self.assertEqual(stats["sent_raw"], 0)
        self.assertEqual(stats["sent_raw_anonymous"], 1)
        self.assertEqual(stats["sent_dlq"], 0)
        self.assertEqual(producer.calls[0]["topic"], "mooc.raw.anonymous.events")

    def test_identity_from_context_user_id_stays_primary_raw(self) -> None:
        producer = _FakeProducer()
        event = {
            "event_type": "play_video",
            "event_source": "browser",
            "name": "play_video",
            "time": "2026-01-01T12:00:00+00:00",
            "context": {"user_id": 9001},
        }
        replay_stream(
            records=[self._record(event, key=b"user:test")],
            producer=producer,
            raw_topic="mooc.raw.events",
            dlq_topic="mooc.dlq.events",
            speed=9999.0,
            dlq_failed_topic="mooc.raw.events",
            filter_fn=is_event_allowed,
            filter_cfg=self.cfg,
            anonymous_topic="mooc.raw.anonymous.events",
        )
        self.assertEqual(producer.calls[0]["topic"], "mooc.raw.events")


class ActorIdentityTest(unittest.TestCase):
    def test_username_present_is_identity(self) -> None:
        self.assertTrue(
            event_has_subject_identity({"username": "u1", "context": {}, "event_type": "x", "event_source": "b"})
        )

    def test_context_user_id_present(self) -> None:
        self.assertTrue(
            event_has_subject_identity(
                {"context": {"user_id": 12}, "event_type": "x", "event_source": "b"}
            )
        )

    def test_missing_both_returns_false(self) -> None:
        self.assertFalse(
            event_has_subject_identity(
                {"username": "", "context": {}, "event_type": "x", "event_source": "b"}
            )
        )


if __name__ == "__main__":
    unittest.main()

