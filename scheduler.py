import sys
import os
import pandas
import traceback

from datetime import datetime, timedelta
from schedule import every, repeat, run_pending
from Location.measurement.measurement import LocationMeter
from Location.translation.translation import create_translation
from Usage.measurement.measurement import UsageMeter
from Activity.status_report import update_status_report, write_report
from Activity.cleanup import fetch_open_session_games, parse_protected_games, run_game_cleanup
from logger_metrics import metrics
from state import data_lock, state

def relative_path(rel_path: str) -> str:
    script_path = os.path.abspath(__file__)
    script_dir = os.path.split(script_path)[0]
    return os.path.join(script_dir, rel_path)


# IPS_DOCUMENTED = relative_path('Location/logs/ip_access_meas.log')

# loc_meter = LocationMeter()
use_meter = UsageMeter()
MEASURING_INTERVAL_MIN: int = int(os.getenv("MEASUREMENT_INTERVAL_MIN", "10"))
MEASURING_INTERVAL_SEC: int = int(os.getenv("MEASUREMENT_HW_INTERVAL_SEC", "1"))
TRANSLATION_TIME: str = os.getenv("TRANSLATION_TIME", "00:00")
SAVING_TIME: str = os.getenv("SAVING_TIME", "00:00")
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
metrics.cleanup_enabled.set(1 if GAME_CLEANUP_ENABLED else 0)


@repeat(every(MEASURING_INTERVAL_SEC).seconds)
def measuring_job_users():
    with metrics.observe_job("session_measurement"):
        use_meter.update_usr_measurement()

@repeat(every(15).seconds)
def measuring_job_cpu():
    with metrics.observe_job("cpu_measurement"):
        use_meter.update_cpu_measurement()

@repeat(every(MEASURING_INTERVAL_MIN).minutes)
def measuring_job_player_retention():
    with metrics.observe_job("game_usage_measurement"):
        with data_lock:
            state.daily_game_user_log = use_meter.update_loc_measurement(state.daily_game_user_log)

@repeat(every(MEASURING_INTERVAL_MIN).minutes)
def measuring_job_maximum_usage():
    with metrics.observe_job("usage_measurement"):
        with data_lock:
            state.daily_hardware_user_log = use_meter.update_hwr_measurements(state.daily_hardware_user_log)
            metrics.record_usage(state.daily_hardware_user_log)
            print(f"Hardware user logs: {state.daily_hardware_user_log}")

@repeat(every().day.at(SAVING_TIME))
def saving_job():
    with metrics.observe_job("usage_saving"):
        with data_lock:
            print("Start: Saving")
            log_date = datetime.today() - timedelta(days=1)
            save_path = relative_path(
                f"Usage/logs/usage-{log_date.strftime('%Y-%m-%d')}.log")
            state.daily_hardware_user_log.to_csv(save_path)
            clear_measurements(state.daily_hardware_user_log, "Daily hardware-user measurements")

@repeat(every().day.at(TRANSLATION_TIME))
def translating_job():
    try:
        with metrics.observe_job("location_translation"):
            with data_lock:
                print("Start: Translating")
                log_date = datetime.today() - timedelta(days=1)
                translation_path = relative_path(
                    f"Location/logs/locations-{log_date.strftime('%Y-%m-%d')}.log")
                translated = create_translation(state.daily_game_user_log, translation_path)
                metrics.record_location_usage(translated)
                clear_measurements(state.daily_game_user_log, "Daily measurements")
    except FileNotFoundError as e:
        print(f"[{datetime.now()}] - Exception during translation: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[{datetime.now()}] - Undefined exception during translation: {e}", file=sys.stderr)

def clear_measurements(doc_df: pandas.DataFrame, message: str) -> None:
    # f = open(ips_documented, 'r+', encoding="utf_8")
    # f.truncate(0)
    doc_df.drop(doc_df.index, inplace=True)
    print(f"[{datetime.now()}] {message} cleared from DataFrame: {doc_df}")

def activity_job():
    if not ACTIVITY_API:
        return
    try:
        with metrics.observe_job("game_cleanup"):
            status_report = update_status_report(
                ACTIVITY_API,
                ACTIVITY_REPORT,
                GAME_INACTIVE_AFTER_DAYS,
                GAME_DELETION_GRACE_DAYS,
            )
            metrics.record_activity_report(status_report)
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
            metrics.record_cleanup_report(cleanup_report)
            print(f"[{datetime.now()}] Updated activity report for {len(status_report['games'])} games.")
    except Exception as e:
        print(f"[{datetime.now()}] - Activity report failed: {e}", file=sys.stderr)


if ACTIVITY_API and ACTIVITY_REPORT_TIME:
    every().day.at(ACTIVITY_REPORT_TIME).do(activity_job)
