from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from kafka.src.adapters.mooc_tracking_log_adapter import (
    _iter_tracking_files,
    iter_mooc_tracking_log_records,
)


class TrackingLogAdapterTest(unittest.TestCase):
    def _collect_records(self, root: Path, **kwargs) -> list:
        with redirect_stdout(io.StringIO()):
            return list(iter_mooc_tracking_log_records(input_root=root, **kwargs))

    def test_discovers_tracking_files_in_sorted_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            names = [
                "tracking.log-20251001-1759310221.json",
                "tracking.log-20251001-1759277821.json",
                "tracking.log-20251001-1759281421.json",
            ]
            for name in names:
                (root / name).write_text("{}", encoding="utf-8")

            files = _iter_tracking_files(root)

        self.assertEqual([path.name for path in files], sorted(names))

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

            records = self._collect_records(root)

        self.assertEqual(len(records), 2)
        self.assertTrue(all(rec.validation_ok for rec in records))
        self.assertEqual(records[0].event["event_type"], "/")
        self.assertEqual(records[1].event["event_type"], "/courses/course-v1:X+Y+Z/course/")

    def test_reads_tracking_file_without_json_suffix(self) -> None:
        payload = """{
    "time": "2026-01-17T21:17:36.701096+00:00",
    "event_type": "/",
    "event_source": "server",
    "username": "alice",
    "session": "",
    "ip": "1.2.3.4"
}
"""

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "tracking.log-20260118-1768702621"
            path.write_text(payload, encoding="utf-8")

            records = self._collect_records(root)

        self.assertEqual(len(records), 1)
        self.assertTrue(records[0].validation_ok)
        self.assertEqual(records[0].event["username"], "alice")

    def test_jsonl_continues_after_malformed_line(self) -> None:
        valid_one = (
            '{"time":"2026-01-17T21:17:36.701096+00:00","event_type":"/",'
            '"event_source":"server","username":"alice","session":"","ip":"1.2.3.4"}'
        )
        invalid = '{"time":"2026-01-17T21:17:37.701096+00:00","event_type":"problem_graded","event":"unterminated}'
        valid_two = (
            '{"time":"2026-01-17T21:17:38.701096+00:00","event_type":"/next",'
            '"event_source":"browser","username":"bob","session":"","ip":"1.2.3.5"}'
        )
        payload = "\n".join([valid_one, invalid, valid_two]) + "\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "tracking.log-20260118-1768702621.json"
            path.write_text(payload, encoding="utf-8")

            records = self._collect_records(root)

        self.assertEqual(len(records), 3)
        self.assertTrue(records[0].validation_ok)
        self.assertEqual(records[1].decode_error is not None, True)
        self.assertTrue(records[2].validation_ok)
        self.assertEqual(records[2].event["username"], "bob")

    def test_can_skip_malformed_jsonl_lines(self) -> None:
        valid_one = (
            '{"time":"2026-01-17T21:17:36.701096+00:00","event_type":"/",'
            '"event_source":"server","username":"alice","session":"","ip":"1.2.3.4"}'
        )
        invalid = '{"time":"2026-01-17T21:17:37.701096+00:00","event_type":"problem_graded","event":"unterminated}'
        valid_two = (
            '{"time":"2026-01-17T21:17:38.701096+00:00","event_type":"/next",'
            '"event_source":"browser","username":"bob","session":"","ip":"1.2.3.5"}'
        )
        payload = "\n".join([valid_one, invalid, valid_two]) + "\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "tracking.log-20260118-1768702621.json"
            path.write_text(payload, encoding="utf-8")

            records = self._collect_records(root, skip_decode_errors=True)

        self.assertEqual(len(records), 2)
        self.assertTrue(all(record.decode_error is None for record in records))
        self.assertEqual(records[1].event["username"], "bob")

    def test_processes_ten_files_without_stopping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            for i in range(10):
                name = f"tracking.log-20251001-{1759277821 + i}.json"
                payload = (
                    '{"time":"2026-01-17T21:17:36.701096+00:00","event_type":"/",'
                    f'"event_source":"server","username":"u{i}","session":"","ip":"1.2.3.{i}"}}\n'
                    '{"time":"2026-01-17T21:17:37.701096+00:00","event_type":"/next",'
                    f'"event_source":"browser","username":"u{i}","session":"","ip":"1.2.3.{i}"}}\n'
                )
                (root / name).write_text(payload, encoding="utf-8")

            records = self._collect_records(root, max_files=10)

        self.assertEqual(len(records), 20)
        self.assertTrue(all(record.validation_ok for record in records))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
