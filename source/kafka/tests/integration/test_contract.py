from __future__ import annotations

import unittest

from src.common import validate_tracking_event


class ContractTest(unittest.TestCase):
    def test_tracking_event_matches_contract(self) -> None:
        event = {
            "time": "2026-01-17T21:36:38.892173+00:00",
            "event_type": "load_video",
            "event_source": "browser",
        }
        valid, reason = validate_tracking_event(event)
        self.assertTrue(valid, reason)

    def test_missing_fields_should_fail_validation(self) -> None:
        event = {"event_type": "x"}
        valid, reason = validate_tracking_event(event)
        self.assertFalse(valid)
        self.assertIn("missing required field", reason)


if __name__ == "__main__":
    unittest.main()
