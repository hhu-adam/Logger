import time
import socket
import threading
import os
from datetime import datetime, timedelta
import scheduler

from api import app
from logger_metrics import metrics
from schedule import run_pending

def wait_for_port(port: int, timeout: float = 10.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                print(f"Service on port {port} started")
                return True
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Service on port {port} did not start within {timeout}s")

metrics_host = os.getenv("LOGGER_METRICS_HOST", "127.0.0.1")
metrics_port = int(os.getenv("LOGGER_METRICS_PORT", "8078"))
api_port = int(os.getenv("LOGGER_API_PORT", "8077"))
metrics.start(metrics_host, metrics_port)
metrics.restore_saved_reports(
    scheduler.ACTIVITY_REPORT if scheduler.ACTIVITY_API else None,
    scheduler.CLEANUP_REPORT if scheduler.ACTIVITY_API else None,
    scheduler.relative_path(
        f"Location/logs/locations-{(datetime.today() - timedelta(days=1)):%Y-%m-%d}.log"
    ),
)
metrics.cleanup_enabled.set(1 if scheduler.GAME_CLEANUP_ENABLED else 0)

api_thread = threading.Thread(
    target=lambda: app.run(host="localhost", port=api_port),
    daemon=True
)

api_thread.start()
wait_for_port(api_port)

while True:
    run_pending()
    time.sleep(1)
