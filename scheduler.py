import sys
import time
import os
import pandas
from datetime import datetime, timedelta
from schedule import every, repeat, run_pending
from Location.measurement.measurement import LocationMeter
from Location.translation.translation import create_translation
from Usage.measurement.measurement import UsageMeter

def relative_path(rel_path: str) -> str:
    script_path = os.path.abspath(__file__)
    script_dir = os.path.split(script_path)[0]
    return os.path.join(script_dir, rel_path)


# IPS_DOCUMENTED = relative_path('Location/logs/ip_access_meas.log')
daily_game_user_log: pandas.DataFrame = pandas.DataFrame({"anon-ip": [], 
                                                          "game": [], 
                                                          "n": []})
daily_hardware_user_log: pandas.DataFrame = pandas.DataFrame({"Timestamp": [], 
                                                          "Max_usr": [], 
                                                          "Max_cpu": [],
                                                          "Max_mem": []})
sec_by_sec_hw_usage: pandas.DataFrame = pandas.DataFrame({"Timestamp": [],
                                                           "CPU": [],
                                                           "MEM": []})
sec_by_sec_user_count: pandas.DataFrame = pandas.DataFrame({"Timestamp": [],
                                                           "Users": []})

loc_meter = LocationMeter()
use_meter = UsageMeter()
MEASURING_INTERVAL_MIN: int = int(os.getenv("MEASUREMENT_INTERVAL_MIN"))
MEASURING_INTERVAL_SEC: int = int(os.getenv("MEASUREMENT_HW_INTERVAL_SEC"))
TRANSLATION_TIME: str = os.getenv("TRANSLATION_TIME")
SAVING_TIME: str = os.getenv("SAVING_TIME")

@repeat(every(MEASURING_INTERVAL_SEC).seconds)
def measuring_job_hardware():
    global sec_by_sec_hw_usage
    sec_by_sec_hw_usage = use_meter.measure_hardware(sec_by_sec_hw_usage)

@repeat(every(MEASURING_INTERVAL_SEC).seconds)
def measuring_job_users():
    global sec_by_sec_user_count
    sec_by_sec_user_count = loc_meter.gather_sec_by_sec_measurements(sec_by_sec_user_count)

@repeat(every(MEASURING_INTERVAL_MIN).minutes)
def measuring_job_player_retention():
    global daily_game_user_log
    daily_game_user_log = loc_meter.update_measurements(daily_game_user_log)

@repeat(every(MEASURING_INTERVAL_MIN).minutes)
def measuring_job_maximum_usage():
    global daily_hardware_user_log
    daily_hardware_user_log = use_meter.update_measurements(daily_hardware_user_log,
                                                            sec_by_sec_hw_usage,
                                                            sec_by_sec_user_count)
    # Clear aggregated user and hardware statistics
    clear_measurements(sec_by_sec_user_count, "Sec by sec user measurements")
    clear_measurements(sec_by_sec_hw_usage, "Sec by sec hardware measurements")

@repeat(every().day.at(SAVING_TIME))
def saving_job():
    global daily_hardware_user_log
    print("Start: Saving")
    log_date = datetime.today() - timedelta(days=1)
    save_path = relative_path(
        f"Usage/logs/usage-{log_date.strftime('%Y-%m-%d')}.log")
    
    daily_hardware_user_log.to_csv(save_path)

    clear_measurements(daily_hardware_user_log, "Daily hardware-user measurements")

@repeat(every().day.at(TRANSLATION_TIME))
def translating_job():
    global daily_game_user_log
    print("Start: Translating")
    translated = False
    log_date = datetime.today() - timedelta(days=1)
    translation_path = relative_path(
        f"Location/logs/locations-{log_date.strftime('%Y-%m-%d')}.log")
    
    try:
        create_translation(daily_game_user_log, translation_path)
        translated = True
    except FileNotFoundError as e:
        print(
            f"Exception during translation: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Undefined exception during translation: {e}", file=sys.stderr)

    if translated:
        clear_measurements(daily_game_user_log, "Daily measurements")

def clear_measurements(doc_df: pandas.DataFrame, message: str) -> None:
    # f = open(ips_documented, 'r+', encoding="utf_8")
    # f.truncate(0)
    doc_df.drop(doc_df.index, inplace=True)
    print(f"[{datetime.now()}] {message} cleared from DataFrame: {doc_df}")

while True:
    run_pending()
    time.sleep(1)
