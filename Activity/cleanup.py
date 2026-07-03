import argparse
import os
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from Activity.status_report import build_status_report, fetch_activity_registry, write_report


COMPONENT_PATTERN = re.compile(r"^[\w.-]+$")


def parse_protected_games(value: str) -> set[str]:
    return {game.strip().lower() for game in value.split(",") if game.strip()}


def fetch_open_session_games(api_url: str) -> set[str]:
    import requests

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    games = payload.get("game")
    if not isinstance(games, list):
        raise ValueError("sessions API response does not contain a game list")
    return {str(game).lower() for game in games}


def safe_game_path(games_dir: Path, game: str) -> Path:
    parts = game.lower().split("/")
    if len(parts) != 2 or not all(COMPONENT_PATTERN.fullmatch(part) for part in parts):
        raise ValueError(f"invalid game identifier: {game}")
    owner, repo = parts

    root = games_dir.resolve()
    owner_path = root / owner
    if owner_path.is_symlink():
        raise ValueError(f"game owner directory is a symlink: {game}")
    candidate = owner_path / repo
    if root not in candidate.resolve().parents:
        raise ValueError(f"game path escapes games directory: {game}")
    return candidate


def find_cleanup_candidates(
    report: dict[str, Any],
    games_dir: str | Path,
    protected_games: set[str] | None = None,
    open_session_games: set[str] | None = None,
) -> list[dict[str, Any]]:
    root = Path(games_dir)
    protected = protected_games or set()
    open_sessions = open_session_games or set()
    candidates: list[dict[str, Any]] = []

    for game, activity in report.get("games", {}).items():
        if activity.get("status") != "deletion_due":
            continue

        item: dict[str, Any] = {
            "game": game,
            "lastActivityAt": activity.get("lastActivityAt"),
            "deletionDueAt": activity.get("deletionDueAt"),
            "action": "eligible",
        }
        if game in protected:
            item.update(action="skipped", reason="protected")
        elif game in open_sessions:
            item.update(action="skipped", reason="currently active session")
        else:
            try:
                source = safe_game_path(root, game)
                item["path"] = str(source)
                if not source.exists():
                    item.update(action="skipped", reason="game directory not found")
                elif source.is_symlink():
                    item.update(action="skipped", reason="game directory is a symlink")
            except ValueError as error:
                item.update(action="skipped", reason=str(error))
        candidates.append(item)

    return candidates


def delete_expired_trash(
    games_dir: str | Path,
    retention_days: int,
    now: datetime,
    apply: bool,
) -> list[str]:
    trash = Path(games_dir).resolve() / ".trash"
    if not trash.exists():
        return []

    cutoff = now.timestamp() - timedelta(days=retention_days).total_seconds()
    expired_trash: list[str] = []
    for entry in trash.iterdir():
        if entry.is_symlink() or not entry.is_dir() or entry.stat().st_mtime > cutoff:
            continue
        expired_trash.append(str(entry))
        if apply:
            shutil.rmtree(entry)
    return expired_trash


def run_game_cleanup(
    report: dict[str, Any],
    games_dir: str | Path,
    protected_games: set[str] | None = None,
    open_session_games: set[str] | None = None,
    apply: bool = False,
    trash_retention_days: int = 7,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    root = Path(games_dir).resolve()
    trash = root / ".trash"

    # Delete only trash entries that existed before this run. Newly moved
    # games always survive at least until the next lifecycle pass.
    expired_trash = delete_expired_trash(root, trash_retention_days, current_time, apply)
    candidates = find_cleanup_candidates(report, root, protected_games, open_session_games)

    if apply:
        trash.mkdir(parents=True, exist_ok=True)
        timestamp = current_time.strftime("%Y%m%dT%H%M%SZ")
        for item in candidates:
            if item["action"] != "eligible":
                continue
            source = Path(item["path"])
            owner, repo = item["game"].split("/", 1)
            target = trash / f"{timestamp}__{owner}__{repo}"
            suffix = 1
            while target.exists():
                target = trash / f"{timestamp}__{owner}__{repo}__{suffix}"
                suffix += 1
            try:
                source.rename(target)
                item.update(action="moved_to_trash", trashPath=str(target))
            except OSError as error:
                item.update(action="skipped", reason=f"move to trash failed: {error}")

    return {
        "generatedAt": current_time.isoformat().replace("+00:00", "Z"),
        "cleanupEnabled": apply,
        "trashRetentionDays": trash_retention_days,
        "candidates": candidates,
        "expiredTrash": expired_trash,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely clean up inactive Lean4Game installations.")
    parser.add_argument("--activity-api", default=os.getenv("ACTIVITY_API", "http://127.0.0.1:8010/api/game-activity"))
    parser.add_argument("--sessions-api", default=os.getenv("SESSIONS_API", os.getenv("API", "")))
    parser.add_argument("--games-dir", default=os.getenv("GAMES_DIR", "../lean4game/games"))
    parser.add_argument("--protected", default=os.getenv("GAME_PROTECTED_REPOS", ""))
    parser.add_argument("--inactive-days", type=int, default=int(os.getenv("GAME_INACTIVE_AFTER_DAYS", "60")))
    parser.add_argument("--grace-days", type=int, default=int(os.getenv("GAME_DELETION_GRACE_DAYS", "30")))
    parser.add_argument("--trash-retention-days", type=int, default=int(os.getenv("GAME_TRASH_RETENTION_DAYS", "7")))
    parser.add_argument("--activity-report", default=os.getenv("ACTIVITY_REPORT", "Activity/logs/activity-status.json"))
    parser.add_argument("--cleanup-report", default=os.getenv("CLEANUP_REPORT", "Activity/logs/cleanup-status.json"))
    parser.add_argument("--apply", action="store_true", help="move deletion-due games to .trash and delete old trash")
    args = parser.parse_args()

    if args.apply and not args.sessions_api:
        parser.error("--sessions-api is required with --apply")

    report = build_status_report(
        fetch_activity_registry(args.activity_api),
        inactive_days=args.inactive_days,
        deletion_grace_days=args.grace_days,
    )
    open_session_games = fetch_open_session_games(args.sessions_api) if args.sessions_api else set()
    cleanup = run_game_cleanup(
        report,
        args.games_dir,
        parse_protected_games(args.protected),
        open_session_games,
        apply=args.apply,
        trash_retention_days=args.trash_retention_days,
    )
    write_report(report, args.activity_report)
    write_report(cleanup, args.cleanup_report)
    print(f"Wrote {args.activity_report}")
    print(f"Wrote {args.cleanup_report}")


if __name__ == "__main__":
    main()
