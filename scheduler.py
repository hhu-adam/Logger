import sys
import os
import pandas
import traceback

from datetime import datetime, timedelta
from schedule import every, repeat, run_pending
from Location.measurement.measurement import LocationMeter
from Location.translation.translation import create_translation
from Usage.measurement.measurement import UsageMeter
from state import data_lock, state

def relative_path(rel_path: str) -> str:
    script_path = os.path.abspath(__file__)
    script_dir = os.path.split(script_path)[0]
    return os.path.join(script_dir, rel_path)


# IPS_DOCUMENTED = relative_path('Location/logs/ip_access_meas.log')

loc_meter = LocationMeter()
use_meter = UsageMeter()

MEASURING_INTERVAL_MIN: int = int(os.getenv("MEASUREMENT_INTERVAL_MIN", "10"))
MEASURING_INTERVAL_SEC: int = int(os.getenv("MEASUREMENT_HW_INTERVAL_SEC", "1"))
TRANSLATION_TIME: str = os.getenv("TRANSLATION_TIME", "00:00")
SAVING_TIME: str = os.getenv("SAVING_TIME", "00:00")


@repeat(every(MEASURING_INTERVAL_SEC).seconds)
def measuring_job_users():
    #try:
    #    with data_lock:
    #        #state.sec_by_sec_user_count = 
    #        loc_meter.gather_sec_by_sec_measurements()
    #except Exception as e:
    #    print(f"[measuring_job_users] Exception: {e}", flush=True)
    #    traceback.print_exc()
    loc_meter.update_sec_by_sec_measurements()

@repeat(every(15).seconds)
def measuring_job_cpu():
    use_meter.update_cpu_measurement()

@repeat(every(MEASURING_INTERVAL_MIN).minutes)
def measuring_job_player_retention():
    with data_lock:
        state.daily_game_user_log = loc_meter.update_measurements(state.daily_game_user_log)

@repeat(every(MEASURING_INTERVAL_MIN).minutes)
def measuring_job_maximum_usage():
    with data_lock:
        state.daily_hardware_user_log = use_meter.update_measurements(state.daily_hardware_user_log)
        print(f"Hardware user logs: {state.daily_hardware_user_log}")
        # Clear aggregated user and hardware statistics
        # clear_measurements(state.sec_by_sec_user_count, "Sec by sec user measurements")

@repeat(every().day.at(SAVING_TIME))
def saving_job():
    with data_lock:
        print("Start: Saving")
        log_date = datetime.today() - timedelta(days=1)
        save_path = relative_path(
            f"Usage/logs/usage-{log_date.strftime('%Y-%m-%d')}.log")
        
        state.daily_hardware_user_log.to_csv(save_path)

        clear_measurements(state.daily_hardware_user_log, "Daily hardware-user measurements")

@repeat(every().day.at(TRANSLATION_TIME))
def translating_job():
    with data_lock:
        print("Start: Translating")
        translated = False
        log_date = datetime.today() - timedelta(days=1)
        translation_path = relative_path(
            f"Location/logs/locations-{log_date.strftime('%Y-%m-%d')}.log")
        
        try:
            create_translation(state.daily_game_user_log, translation_path)
            translated = True
        except FileNotFoundError as e:
            print(
                f"[{datetime.now()}] Exception during translation: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Undefined exception during translation: {e}", file=sys.stderr)

        if translated:
            clear_measurements(state.daily_game_user_log, "Daily measurements")

def clear_measurements(doc_df: pandas.DataFrame, message: str) -> None:
    # f = open(ips_documented, 'r+', encoding="utf_8")
    # f.truncate(0)
    doc_df.drop(doc_df.index, inplace=True)
    print(f"[{datetime.now()}] {message} cleared from DataFrame: {doc_df}")

#while True:
#    run_pending()
#    time.sleep(1)
