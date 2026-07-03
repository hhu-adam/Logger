import sys
import time
import os
import pandas
from datetime import datetime, timedelta
from schedule import every, repeat, run_pending
from Location.measurement.measurement import update_measurements
from Location.translation.translation import create_translation, clear_daily_measurements
from Activity.status_report import update_status_report, write_report
from Activity.cleanup import fetch_open_session_games, parse_protected_games, run_game_cleanup


def relative_path(rel_path: str) -> str:
    script_path = os.path.abspath(__file__)
    script_dir = os.path.split(script_path)[0]
    return os.path.join(script_dir, rel_path)


# IPS_DOCUMENTED = relative_path('Location/logs/ip_access_meas.log')
daily_log: pandas.DataFrame = pandas.DataFrame({"anon-ip": [], "game": [], "n": []})
MEASURING_INTERVAL: int = int(os.getenv("MEASUREMENT_INTERVAL_MIN"))
TRANSLATION_TIME: str = os.getenv("TRANSLATION_TIME")
ACTIVITY_API: str | None = os.getenv("ACTIVITY_API")
ACTIVITY_REPORT_TIME: str | None = os.getenv("ACTIVITY_REPORT_TIME")
ACTIVITY_REPORT: str = os.getenv(
    "ACTIVITY_REPORT", relative_path("Activity/logs/activity-status.json")
)
GAME_INACTIVE_AFTER_DAYS: int = int(os.getenv("GAME_INACTIVE_AFTER_DAYS", "60"))
GAME_DELETION_GRACE_DAYS: int = int(os.getenv("GAME_DELETION_GRACE_DAYS", "30"))
GAME_CLEANUP_ENABLED: bool = os.getenv("GAME_CLEANUP_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
GAMES_DIR: str = os.getenv("GAMES_DIR", relative_path("../lean4game/games"))
SESSIONS_API: str = os.getenv("SESSIONS_API", os.getenv("API", ""))
GAME_PROTECTED_REPOS: set[str] = parse_protected_games(os.getenv("GAME_PROTECTED_REPOS", ""))
GAME_TRASH_RETENTION_DAYS: int = int(os.getenv("GAME_TRASH_RETENTION_DAYS", "7"))
CLEANUP_REPORT: str = os.getenv(
    "CLEANUP_REPORT", relative_path("Activity/logs/cleanup-status.json")
)

@repeat(every(MEASURING_INTERVAL).minutes)
def measuring_job():
    global daily_log
    daily_log = update_measurements(daily_log)


@repeat(every().day.at(TRANSLATION_TIME))
def translating_job():
    global daily_log
    print("Start: Translating")
    translated = False
    log_date = datetime.today() - timedelta(days=1)
    translation_path = relative_path(
        f"Location/logs/locations-{log_date.strftime('%Y-%m-%d')}.log")
    
    try:
        create_translation(daily_log, translation_path)
        translated = True
    except FileNotFoundError as e:
        print(
            f"[{datetime.now()}] - Exception during translation: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[{datetime.now()}] - Undefined exception during translation: {e}", file=sys.stderr)

    if translated:
        clear_daily_measurements(daily_log)


def activity_job():
    if not ACTIVITY_API:
        return
    try:
        status_report = update_status_report(
            ACTIVITY_API,
            ACTIVITY_REPORT,
            GAME_INACTIVE_AFTER_DAYS,
            GAME_DELETION_GRACE_DAYS,
        )
        if GAME_CLEANUP_ENABLED and not SESSIONS_API:
            raise ValueError("SESSIONS_API is required when GAME_CLEANUP_ENABLED=true")
        open_session_games = fetch_open_session_games(SESSIONS_API) if SESSIONS_API else set()
        cleanup_report = run_game_cleanup(
            status_report,
            GAMES_DIR,
            GAME_PROTECTED_REPOS,
            open_session_games,
            apply=GAME_CLEANUP_ENABLED,
            trash_retention_days=GAME_TRASH_RETENTION_DAYS,
        )
        write_report(cleanup_report, CLEANUP_REPORT)
        print(f"[{datetime.now()}] Updated activity report for {len(status_report['games'])} games.")
    except Exception as e:
        print(f"[{datetime.now()}] - Activity report failed: {e}", file=sys.stderr)


if ACTIVITY_API and ACTIVITY_REPORT_TIME:
    every().day.at(ACTIVITY_REPORT_TIME).do(activity_job)


while True:
    run_pending()
    time.sleep(1)
