import unittest
from unittest.mock import patch

import scheduler


class ActivityRefreshTest(unittest.TestCase):
    def test_refresh_updates_lifecycle_without_running_cleanup(self):
        report = {"games": {"owner/game": {"status": "active"}}}
        with (
            patch.object(scheduler, "ACTIVITY_API", "http://example.invalid/activity"),
            patch.object(scheduler, "update_status_report", return_value=report) as update,
            patch.object(scheduler.metrics, "record_activity_report") as record,
            patch.object(scheduler, "run_game_cleanup") as cleanup,
        ):
            scheduler.activity_refresh_job()

        update.assert_called_once_with(
            "http://example.invalid/activity",
            scheduler.ACTIVITY_REPORT,
            scheduler.GAME_INACTIVE_AFTER_DAYS,
            scheduler.GAME_DELETION_GRACE_DAYS,
        )
        record.assert_called_once_with(report)
        cleanup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
