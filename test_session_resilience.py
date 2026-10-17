"""A Relay outage must leave Logger alive with its accumulated data intact."""

from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import URLError
from urllib.request import urlopen

import pandas
from pandas.testing import assert_frame_equal
from prometheus_client import CollectorRegistry
from prometheus_client.parser import text_string_to_metric_families
import requests
import schedule

from Location.measurement.measurement import LocationMeter
from logger_metrics import LoggerMetrics
import scheduler


class RelayHandler(BaseHTTPRequestHandler):
    mode = "ok"

    def do_GET(self):
        if self.mode == "unavailable":
            self.send_response(503)
            body = b"restarting"
        else:
            self.send_response(200)
            body = b"invalid json" if self.mode == "invalid" else json.dumps({
                "date": ["2026-10-17"], "anon_Ip": ["test-address"],
                "game": ["owner/game"], "lang": ["en"],
            }).encode()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class SessionResilienceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), RelayHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        RelayHandler.mode = "ok"
        self.meter = LocationMeter()
        self.meter.API = f"http://127.0.0.1:{self.server.server_port}/sessions"
        self.meter.sec_by_sec_measurement = pandas.DataFrame({
            "Timestamp": [pandas.Timestamp.now()], "Users": [7.0],
        })
        self.daily = pandas.DataFrame({"anon-ip": ["test-address"], "game": ["owner/game"], "n": [6]})
        self.hardware = pandas.DataFrame({"Timestamp": ["26-10-17 12:00:00"],
                                         "Max_usr": [7.0], "Max_cpu": [10.0], "Max_mem": [40.0]})
        self.metrics = LoggerMetrics(CollectorRegistry())
        for context in (
            patch.object(scheduler.use_meter, "loc_meter", self.meter),
            patch.object(scheduler, "metrics", self.metrics),
            patch.object(scheduler.state, "daily_game_user_log", self.daily),
            patch.object(scheduler.state, "daily_hardware_user_log", self.hardware),
            patch.object(scheduler.state, "reporting_location_period_start", "2026-10-17"),
            patch.object(scheduler.state, "reporting_last_location_measurement", "2026-10-17T12:00:00+00:00"),
            redirect_stderr(StringIO()), redirect_stdout(StringIO()),
        ):
            context.__enter__()
            self.addCleanup(context.__exit__, None, None, None)

    def run_jobs(self):
        runner = schedule.Scheduler()
        jobs = [runner.every().second.do(scheduler.measuring_job_users),
                runner.every().second.do(scheduler.measuring_job_player_retention)]
        continued = []
        jobs.append(runner.every().second.do(lambda: continued.append(True)))
        for job in jobs:
            job.next_run = datetime.now() - timedelta(seconds=1)
        runner.run_pending()
        self.assertEqual(continued, [True])
        self.assertTrue(all(job.next_run > datetime.now() for job in jobs))

    def assert_failed_samples_preserve_data(self):
        self.assertIs(scheduler.state.daily_game_user_log, self.daily)
        self.assertIs(scheduler.state.daily_hardware_user_log, self.hardware)
        self.assertEqual(self.daily.iloc[0]["n"], 6)
        self.assertEqual(scheduler.state.reporting_last_location_measurement, "2026-10-17T12:00:00+00:00")
        self.assertEqual(scheduler.state.reporting_location_period_start, "2026-10-17")
        self.assertEqual(list(self.meter.sec_by_sec_measurement["Users"]), [7.0])
        for name in ("session_measurement", "game_usage_measurement"):
            self.assertEqual(self.metrics.job_failures.labels(name)._value.get(), 1)
            self.assertEqual(self.metrics.job_last_success.labels(name)._value.get(), 0)

    def test_connection_refused_preserves_data_and_does_not_stop_other_jobs(self):
        with patch("Location.measurement.measurement.requests.get", side_effect=requests.ConnectionError("refused")):
            self.run_jobs()
        self.assert_failed_samples_preserve_data()

    def test_request_timeout_preserves_data_and_does_not_stop_other_jobs(self):
        with patch("Location.measurement.measurement.requests.get", side_effect=requests.Timeout("timed out")) as get:
            self.run_jobs()
        self.assertEqual(get.call_args.kwargs["timeout"], (2, 5))
        self.assert_failed_samples_preserve_data()

    def test_http_outage_then_recovery_keeps_previous_observations(self):
        RelayHandler.mode = "unavailable"
        self.run_jobs()
        self.assert_failed_samples_preserve_data()
        RelayHandler.mode = "ok"
        self.run_jobs()
        self.assertEqual(list(self.meter.sec_by_sec_measurement["Users"]), [7.0, 1.0])
        self.assertEqual(scheduler.state.daily_game_user_log.iloc[0]["n"], 7)
        self.assertEqual(self.daily.iloc[0]["n"], 6)
        for name in ("session_measurement", "game_usage_measurement"):
            self.assertGreater(self.metrics.job_last_success.labels(name)._value.get(), 0)
            self.assertEqual(self.metrics.job_failures.labels(name)._value.get(), 1)

    def test_invalid_response_preserves_data_and_does_not_stop_other_jobs(self):
        RelayHandler.mode = "invalid"
        self.run_jobs()
        self.assert_failed_samples_preserve_data()

    def test_missing_session_fields_preserve_data(self):
        with patch.object(self.meter, "get_measurement", return_value={"unexpected": []}):
            self.run_jobs()
        self.assert_failed_samples_preserve_data()

    def test_expired_user_samples_are_unknown_without_erasing_the_buffer(self):
        self.meter.sec_by_sec_measurement = pandas.DataFrame({
            "Timestamp": [pandas.Timestamp.now() - pandas.Timedelta(minutes=11), pandas.Timestamp.now()],
            "Users": [99.0, -1.0],
        })
        before = self.meter.sec_by_sec_measurement.copy(deep=True)
        self.assertTrue(math.isnan(self.meter.get_max_users_over_ten_minutes()))
        assert_frame_equal(self.meter.sec_by_sec_measurement, before)

    def test_a_real_zero_user_sample_is_valid(self):
        self.meter.sec_by_sec_measurement["Users"] = 0.0
        self.assertEqual(self.meter.get_max_users_over_ten_minutes(), 0)

    def test_running_logger_process_survives_outage_and_resumes_sampling(self):
        # Start an isolated Logger, not the production process, with no external
        # services or archive writes. Exercise the actual main.py scheduler loop.
        with socket.socket() as api_socket, socket.socket() as metrics_socket:
            api_socket.bind(("127.0.0.1", 0))
            metrics_socket.bind(("127.0.0.1", 0))
            api_port = api_socket.getsockname()[1]
            metrics_port = metrics_socket.getsockname()[1]
        RelayHandler.mode = "unavailable"
        save_time = (datetime.now(timezone.utc) + timedelta(hours=12)).strftime("%H:%M")
        env = dict(os.environ, API=self.meter.API, ACTIVITY_API="",
                   PROMETHEUS_URL="http://127.0.0.1:1", TZ="UTC",
                   LOGGER_API_HOST="127.0.0.1", LOGGER_API_PORT=str(api_port),
                   LOGGER_METRICS_HOST="127.0.0.1", LOGGER_METRICS_PORT=str(metrics_port),
                   MEASUREMENT_INTERVAL_MIN="10", MEASUREMENT_HW_INTERVAL_SEC="1",
                   SAVING_TIME=save_time, TRANSLATION_TIME=save_time)
        with tempfile.TemporaryFile() as output:
            process = subprocess.Popen([sys.executable, "main.py"],
                                       cwd=Path(__file__).resolve().parent, env=env,
                                       stdout=output, stderr=output)
            try:
                def wait_for_metric(name, minimum):
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        self.assertIsNone(process.poll(), "Logger exited during source outage")
                        try:
                            with urlopen(f"http://127.0.0.1:{metrics_port}/metrics", timeout=1) as response:
                                body = response.read().decode()
                            for family in text_string_to_metric_families(body):
                                for sample in family.samples:
                                    if sample.name == name and sample.labels.get("job") == "session_measurement" and sample.value >= minimum:
                                        return
                        except URLError:
                            pass
                        time.sleep(0.05)
                    self.fail(f"Metric {name} did not reach {minimum}")

                wait_for_metric("logger_job_failures_total", 1)
                RelayHandler.mode = "ok"
                wait_for_metric("logger_job_last_success_timestamp_seconds", 1)
                self.assertIsNone(process.poll())
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
