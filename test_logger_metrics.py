import unittest

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
