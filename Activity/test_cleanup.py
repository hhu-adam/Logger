import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from Activity.cleanup import find_cleanup_candidates, run_game_cleanup, safe_game_path


NOW = datetime(2026, 6, 19, tzinfo=timezone.utc)


def due_report(*games: str):
    return {
        "games": {
            game: {
                "status": "deletion_due",
                "lastActivityAt": "2026-01-01T00:00:00Z",
                "deletionDueAt": "2026-04-01T00:00:00Z",
            }
            for game in games
        }
    }


class CleanupTest(unittest.TestCase):
    def test_cleanup_disabled_never_moves_a_game(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "owner" / "repo"
            game.mkdir(parents=True)
            result = run_game_cleanup(due_report("owner/repo"), directory, now=NOW)
            self.assertTrue(game.exists())
            self.assertEqual(result["candidates"][0]["action"], "eligible")

    def test_apply_moves_game_to_trash(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "owner" / "repo"
            game.mkdir(parents=True)
            result = run_game_cleanup(due_report("owner/repo"), directory, apply=True, now=NOW)
            candidate = result["candidates"][0]
            self.assertFalse(game.exists())
            self.assertEqual(candidate["action"], "moved_to_trash")
            self.assertTrue(Path(candidate["trashPath"]).exists())

    def test_protected_and_open_session_games_are_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            for game in ("owner/protected", "owner/playing"):
                safe_game_path(Path(directory), game).mkdir(parents=True)
            candidates = find_cleanup_candidates(
                due_report("owner/protected", "owner/playing"),
                directory,
                protected_games={"owner/protected"},
                open_session_games={"owner/playing"},
            )
            self.assertEqual([item["action"] for item in candidates], ["skipped", "skipped"])

    def test_local_games_can_be_cleanup_candidates_but_invalid_identifiers_are_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            safe_game_path(Path(directory), "local/robo").mkdir(parents=True)
            candidates = find_cleanup_candidates(
                due_report("local/robo", "../outside/repo"), directory
            )
            self.assertEqual(candidates[0]["action"], "eligible")
            self.assertEqual(candidates[1]["action"], "skipped")


if __name__ == "__main__":
    unittest.main()
