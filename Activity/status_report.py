import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_INACTIVE_DAYS = 60
DEFAULT_DELETION_GRACE_DAYS = 30


def parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def last_activity_at(activity: dict[str, Any]) -> datetime:
    timestamps = [
        parse_timestamp(activity.get("lastImportedAt")),
        parse_timestamp(activity.get("lastPlayedAt")),
        parse_timestamp(activity.get("firstSeenAt")),
    ]
    available = [timestamp for timestamp in timestamps if timestamp is not None]
    if not available:
        raise ValueError("activity record has no usable timestamp")
    return max(available)


def classify_game_status(
    activity: dict[str, Any],
    now: datetime,
    inactive_days: int = DEFAULT_INACTIVE_DAYS,
    deletion_grace_days: int = DEFAULT_DELETION_GRACE_DAYS,
) -> dict[str, Any]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    latest = last_activity_at(activity)
    inactive_at = latest + timedelta(days=inactive_days)
    deletion_due_at = inactive_at + timedelta(days=deletion_grace_days)

    if now >= deletion_due_at:
        status = "deletion_due"
    elif now >= inactive_at:
        status = "inactive"
    else:
        status = "active"

    return {
        **activity,
        "lastActivityAt": latest.isoformat().replace("+00:00", "Z"),
        "inactiveAt": inactive_at.isoformat().replace("+00:00", "Z"),
        "deletionDueAt": deletion_due_at.isoformat().replace("+00:00", "Z"),
        "status": status,
    }


def build_status_report(
    games: dict[str, dict[str, Any]],
    now: datetime | None = None,
    inactive_days: int = DEFAULT_INACTIVE_DAYS,
    deletion_grace_days: int = DEFAULT_DELETION_GRACE_DAYS,
) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc)
    classified = {
        game.lower(): classify_game_status(
            activity, current_time, inactive_days, deletion_grace_days
        )
        for game, activity in sorted(games.items())
    }
    return {
        "generatedAt": current_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "policy": {
            "inactiveAfterDays": inactive_days,
            "deletionGraceDays": deletion_grace_days,
        },
        "games": classified,
    }


def fetch_activity_registry(api_url: str) -> dict[str, dict[str, Any]]:
    import requests

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    games = payload.get("games")
    if not isinstance(games, dict):
        raise ValueError("activity API response does not contain a games object")
    return games


def write_report(report: dict[str, Any], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    temporary_path.replace(path)


def update_status_report(
    api_url: str,
    output_path: str | Path,
    inactive_days: int = DEFAULT_INACTIVE_DAYS,
    deletion_grace_days: int = DEFAULT_DELETION_GRACE_DAYS,
) -> dict[str, Any]:
    report = build_status_report(
        fetch_activity_registry(api_url),
        inactive_days=inactive_days,
        deletion_grace_days=deletion_grace_days,
    )
    write_report(report, output_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a report of Lean4Game activity states.")
    parser.add_argument(
        "--api",
        default=os.getenv("ACTIVITY_API", "http://127.0.0.1:8010/api/game-activity"),
    )
    parser.add_argument(
        "--output",
        default=os.getenv("ACTIVITY_REPORT", "Activity/logs/activity-status.json"),
    )
    parser.add_argument(
        "--inactive-days",
        type=int,
        default=int(os.getenv("GAME_INACTIVE_AFTER_DAYS", DEFAULT_INACTIVE_DAYS)),
    )
    parser.add_argument(
        "--grace-days",
        type=int,
        default=int(os.getenv("GAME_DELETION_GRACE_DAYS", DEFAULT_DELETION_GRACE_DAYS)),
    )
    args = parser.parse_args()

    report = update_status_report(
        args.api, args.output, args.inactive_days, args.grace_days
    )
    counts = {status: 0 for status in ("active", "inactive", "deletion_due")}
    for activity in report["games"].values():
        counts[activity["status"]] += 1
    print(
        f"Wrote {args.output}: {counts['active']} active, "
        f"{counts['inactive']} inactive, {counts['deletion_due']} deletion due."
    )


if __name__ == "__main__":
    main()
