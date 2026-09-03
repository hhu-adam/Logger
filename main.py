import time
import socket
import threading
import os
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
metrics.start(metrics_host, metrics_port)

api_thread = threading.Thread(
    target=lambda: app.run(host="localhost", port=8077),
    daemon=True
)

api_thread.start()
wait_for_port(8077)

while True:
    run_pending()
    time.sleep(1)
