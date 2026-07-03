import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from Activity.cleanup import run_game_cleanup


class TrashCleanupTest(unittest.TestCase):
    def test_expired_trash_is_deleted_only_when_cleanup_runs(self):
        now = datetime(2026, 6, 19, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            expired_entry = Path(directory) / ".trash" / "old-game"
            expired_entry.mkdir(parents=True)
            timestamp = (now - timedelta(days=8)).timestamp()
            os.utime(expired_entry, (timestamp, timestamp))

            dry_run = run_game_cleanup(
                {"games": {}}, directory, apply=False, trash_retention_days=7, now=now
            )
            self.assertTrue(expired_entry.exists())
            self.assertEqual(dry_run["expiredTrash"], [str(expired_entry)])

            applied = run_game_cleanup(
                {"games": {}}, directory, apply=True, trash_retention_days=7, now=now
            )
            self.assertFalse(expired_entry.exists())
            self.assertEqual(applied["expiredTrash"], [str(expired_entry)])


if __name__ == "__main__":
    unittest.main()
