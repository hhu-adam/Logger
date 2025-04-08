import sys
import time
import os
from datetime import datetime, timedelta
from schedule import every, repeat, run_pending
from Location.measurement.measurement import update_measurements
from Location.translation.translation import create_translation, clear_daily_measurements


def relative_path(rel_path: str) -> str:
    script_path = os.path.abspath(__file__)
    script_dir = os.path.split(script_path)[0]
    return os.path.join(script_dir, rel_path)


IPS_DOCUMENTED = relative_path('Location/logs/ip_access_meas.log')


@repeat(every(10).minutes)
def measuring_job():
    print("Start: Measuring")
    ips_measured_path = os.environ.get("IPS_LOG")
    update_measurements(IPS_DOCUMENTED, ips_measured_path)


@repeat(every().day.at("00:00"))
def translating_job():
    print("Start: Translating")
    translated = False
    log_date = datetime.today() - timedelta(days=1)
    ips_translated = relative_path(
        f"Location/logs/locations-{log_date.strftime('%Y-%m-%d')}.log")
    try:
        create_translation(IPS_DOCUMENTED, ips_translated)
        translated = True
    except FileNotFoundError as e:
        print(
            f"Exception during translation: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Undefined exception during translation: {e}", file=sys.stderr)

    if translated:
        clear_daily_measurements(IPS_DOCUMENTED)


while True:
    run_pending()
    time.sleep(1)
