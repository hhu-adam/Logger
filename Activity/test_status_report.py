import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from Activity.status_report import build_status_report, write_report


NOW = datetime(2026, 6, 19, tzinfo=timezone.utc)


class StatusReportTest(unittest.TestCase):
    def test_classifies_active_inactive_and_deletion_due_games(self):
        games = {
            "Example/Active": {
                "firstSeenAt": "2026-01-01T00:00:00Z",
                "firstImportedAt": "2026-01-01T00:00:00Z",
                "lastImportedAt": "2026-06-18T00:00:00Z",
                "lastPlayedAt": "2026-03-01T00:00:00Z",
            },
            "Example/Inactive": {
                "firstSeenAt": "2026-04-01T00:00:00Z",
                "firstImportedAt": None,
                "lastImportedAt": None,
                "lastPlayedAt": "2026-04-01T00:00:00Z",
            },
            "Example/Old": {
                "firstSeenAt": "2026-01-01T00:00:00Z",
                "firstImportedAt": None,
                "lastImportedAt": None,
                "lastPlayedAt": None,
            },
        }

        report = build_status_report(games, now=NOW)

        self.assertEqual(report["games"]["example/active"]["status"], "active")
        self.assertEqual(report["games"]["example/inactive"]["status"], "inactive")
        self.assertEqual(report["games"]["example/old"]["status"], "deletion_due")

    def test_latest_play_or_import_wins(self):
        report = build_status_report(
            {
                "example/game": {
                    "firstSeenAt": "2025-01-01T00:00:00Z",
                    "firstImportedAt": "2025-01-01T00:00:00Z",
                    "lastImportedAt": "2026-01-01T00:00:00Z",
                    "lastPlayedAt": "2026-06-18T00:00:00Z",
                }
            },
            now=NOW,
        )
        game = report["games"]["example/game"]
        self.assertEqual(game["lastActivityAt"], "2026-06-18T00:00:00Z")
        self.assertEqual(game["status"], "active")

    def test_report_write_is_valid_json(self):
        report = build_status_report({}, now=NOW)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "activity.json"
            write_report(report, output)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), report)


if __name__ == "__main__":
    unittest.main()
