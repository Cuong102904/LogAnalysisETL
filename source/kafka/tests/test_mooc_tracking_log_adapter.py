from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kafka.src.adapters.mooc_tracking_log_adapter import iter_mooc_tracking_log_records


class TrackingLogAdapterTest(unittest.TestCase):
    def test_parses_pretty_printed_objects(self) -> None:
        payload = """{
    "time": "2026-01-17T21:17:36.701096+00:00",
    "event_type": "/",
    "event_source": "server",
    "username": "",
    "session": "",
    "ip": "1.2.3.4"
}

{
    "time": "2026-01-17T21:18:48.275114+00:00",
    "event_type": "/courses/course-v1:X+Y+Z/course/",
    "event_source": "browser",
    "username": "alice",
    "session": "",
    "ip": "1.2.3.5"
}
"""

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "tracking.log-0001.json"
            path.write_text(payload, encoding="utf-8")

            records = list(iter_mooc_tracking_log_records(input_root=root))

        self.assertEqual(len(records), 2)
        self.assertTrue(all(rec.validation_ok for rec in records))
        self.assertEqual(records[0].event["event_type"], "/")
        self.assertEqual(records[1].event["event_type"], "/courses/course-v1:X+Y+Z/course/")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
