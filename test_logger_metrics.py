import unittest
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas
from prometheus_client import CollectorRegistry, generate_latest

from logger_metrics import LoggerMetrics


class LoggerMetricsTest(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        self.metrics = LoggerMetrics(self.registry)

    def rendered_metrics(self) -> str:
        return generate_latest(self.registry).decode("utf-8")

    def test_exports_usage_and_job_health(self):
        with self.metrics.observe_job("usage_measurement"):
            self.metrics.record_usage(
                pandas.DataFrame(
                    {
                        "Timestamp": ["2026-09-03 12:00:00"],
                        "Max_usr": [12],
                        "Max_cpu": [45.5],
                        "Max_mem": [67.25],
                    }
                )
            )

        rendered = self.rendered_metrics()
        self.assertIn("logger_usage_peak_players 12.0", rendered)
        self.assertIn("logger_usage_peak_cpu_percent 45.5", rendered)
        self.assertIn("logger_usage_peak_memory_percent 67.25", rendered)
        self.assertIn('logger_job_last_success_timestamp_seconds{job="usage_measurement"}', rendered)

    def test_accepts_usage_meter_two_digit_year_timestamp(self):
        self.metrics.record_usage(
            pandas.DataFrame(
                {
                    "Timestamp": ["26-09-28 18:07:05"],
                    "Max_usr": [12],
                    "Max_cpu": [45.5],
                    "Max_mem": [67.25],
                }
            )
        )
        expected = datetime.strptime("26-09-28 18:07:05", "%y-%m-%d %H:%M:%S").astimezone().timestamp()
        self.assertEqual(self.metrics.usage_measurement.collect()[0].samples[0].value, expected)

    def test_restores_recent_daily_reports_after_restart(self):
        with TemporaryDirectory() as directory:
            activity = Path(directory) / "activity.json"
            cleanup = Path(directory) / "cleanup.json"
            location = Path(directory) / "locations.log"
            generated_at = datetime.now(timezone.utc).isoformat()
            activity.write_text(json.dumps({
                "generatedAt": generated_at,
                "games": {"owner/game": {"status": "active"}},
            }), encoding="utf-8")
            cleanup.write_text(json.dumps({
                "generatedAt": generated_at,
                "cleanupEnabled": False,
                "candidates": [{"action": "eligible"}],
                "expiredTrash": [],
            }), encoding="utf-8")
            location.write_text("country;game;n\nDE;owner/game;6\n", encoding="utf-8")

            self.metrics.restore_saved_reports(activity, cleanup, location)

        rendered = self.rendered_metrics()
        self.assertIn('logger_game_lifecycle_games{status="active"} 1.0', rendered)
        self.assertIn('logger_game_cleanup_candidates{action="eligible"} 1.0', rendered)
        self.assertIn('logger_location_game_observations{country="DE",game="owner/game"} 6.0', rendered)

    def test_does_not_restore_stale_activity_report(self):
        with TemporaryDirectory() as directory:
            activity = Path(directory) / "activity.json"
            activity.write_text(json.dumps({
                "generatedAt": (datetime.now(timezone.utc) - timedelta(days=3)).isoformat(),
                "games": {"owner/game": {"status": "active"}},
            }), encoding="utf-8")

            self.metrics.restore_saved_reports(activity, None, Path(directory) / "missing.log")

        self.assertNotIn('logger_game_lifecycle_games{status="active"}', self.rendered_metrics())

    def test_exports_lifecycle_and_cleanup_without_paths(self):
        self.metrics.record_activity_report(
            {
                "games": {
                    "owner/active": {
                        "status": "active",
                        "lastActivityAt": "2026-09-02T00:00:00Z",
                        "inactiveAt": "2026-11-01T00:00:00Z",
                        "deletionDueAt": "2026-12-01T00:00:00Z",
                    },
                    "owner/due": {
                        "status": "deletion_due",
                        "lastActivityAt": "2026-01-01T00:00:00Z",
                        "inactiveAt": "2026-03-01T00:00:00Z",
                        "deletionDueAt": "2026-04-01T00:00:00Z",
                    },
                }
            }
        )
        self.metrics.record_cleanup_report(
            {
                "cleanupEnabled": True,
                "candidates": [
                    {"action": "moved_to_trash", "path": "/private/game"},
                    {"action": "skipped", "reason": "protected"},
                ],
                "expiredTrash": ["/private/trash"],
            }
        )

        rendered = self.rendered_metrics()
        self.assertIn('logger_game_lifecycle_games{status="active"} 1.0', rendered)
        self.assertIn('logger_game_lifecycle_games{status="deletion_due"} 1.0', rendered)
        self.assertIn('logger_game_lifecycle_info{game="owner/due",status="deletion_due"} 1.0', rendered)
        self.assertIn('logger_game_cleanup_candidates{action="moved_to_trash"} 1.0', rendered)
        self.assertIn('logger_game_cleanup_skipped_games{reason="protected"} 1.0', rendered)
        self.assertNotIn("/private", rendered)

    def test_exports_aggregated_country_game_usage_only(self):
        self.metrics.record_location_usage(
            pandas.DataFrame(
                {
                    "country": ["DE", "US"],
                    "game": ["owner/game", "owner/game"],
                    "n": [6, 4],
                }
            )
        )

        rendered = self.rendered_metrics()
        self.assertIn('logger_location_game_observations{country="DE",game="owner/game"} 6.0', rendered)
        self.assertIn('logger_location_game_observations{country="US",game="owner/game"} 4.0', rendered)
        self.assertNotIn("anon-ip", rendered)


if __name__ == "__main__":
    unittest.main()
