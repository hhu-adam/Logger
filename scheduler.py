import sys
import time
import os
import pandas
from datetime import datetime, timedelta
from schedule import every, repeat, run_pending
from Location.measurement.measurement import update_measurements
from Location.translation.translation import create_translation, clear_daily_measurements


def relative_path(rel_path: str) -> str:
    script_path = os.path.abspath(__file__)
    script_dir = os.path.split(script_path)[0]
    return os.path.join(script_dir, rel_path)


# IPS_DOCUMENTED = relative_path('Location/logs/ip_access_meas.log')
daily_log: pandas.DataFrame = pandas.DataFrame({"anon-ip": [], "game": [], "n": []})
MEASURING_INTERVAL: int = int(os.getenv("MEASUREMENT_INTERVAL_MIN"))
TRANSLATION_TIME: str = os.getenv("TRANSLATION_TIME")

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
            f"Exception during translation: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Undefined exception during translation: {e}", file=sys.stderr)

    if translated:
        clear_daily_measurements(daily_log)


while True:
    run_pending()
    time.sleep(1)
