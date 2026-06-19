import time
import threading
import scheduler

from api import app
from schedule import run_pending

api_thread = threading.Thread(
    target=lambda: app.run(host="localhost", port=8077),
    daemon=True
)

api_thread.start()

while True:
    run_pending()
    time.sleep(1)