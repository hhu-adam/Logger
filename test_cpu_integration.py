"""Verify CPU-unavailable data survives the usage pipeline and API."""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import math
import unittest
from unittest.mock import patch

import pandas
from prometheus_client import CollectorRegistry

from api import create_app
from logger_metrics import LoggerMetrics
import scheduler


class CpuIntegrationTest(unittest.TestCase):
    def test_usage_latest_returns_json_null_for_unavailable_cpu(self):
        measurement = pandas.DataFrame({"Timestamp": ["26-10-17 12:00:00"],
                                        "Max_usr": [12], "Max_cpu": [math.nan], "Max_mem": [50]})
        with patch.object(scheduler.use_meter, "get_measurement", return_value=measurement):
            response = create_app().test_client().get("/usage/latest")
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json["Max_cpu"])
        self.assertEqual(response.json["Max_mem"], 50)
        self.assertNotIn(b"NaN", response.data)

    def test_usage_measurement_keeps_ram_players_and_persists_cpu_as_blank(self):
        meter = scheduler.use_meter
        with (
            patch.object(meter.cpu_meter, "get_max_cpu_usage_over_last_ten_min", return_value=math.nan),
            patch.object(meter.ram_meter, "get_max_ram_usage_over_ten_min", return_value=50),
            patch.object(meter.loc_meter, "get_max_users_over_ten_minutes", return_value=12),
            redirect_stdout(StringIO()),
        ):
            measurement = meter.get_measurement()
        self.assertTrue(math.isnan(measurement.iloc[0]["Max_cpu"]))
        self.assertEqual(measurement.iloc[0]["Max_mem"], 50)
        self.assertEqual(measurement.iloc[0]["Max_usr"], 12)
        self.assertNotIn("nan", measurement.to_csv(index=False).lower())

    def test_usage_source_failure_keeps_scheduler_running_and_records_failure(self):
        metrics = LoggerMetrics(CollectorRegistry())
        original = scheduler.state.daily_hardware_user_log
        with (
            patch.object(scheduler, "metrics", metrics),
            patch.object(scheduler.use_meter, "update_hwr_measurements", side_effect=ConnectionError("offline")),
            redirect_stderr(StringIO()),
        ):
            scheduler.measuring_job_maximum_usage()
        self.assertIs(scheduler.state.daily_hardware_user_log, original)
        self.assertEqual(metrics.job_failures.labels("usage_measurement")._value.get(), 1)
        self.assertEqual(metrics.job_last_success.labels("usage_measurement")._value.get(), 0)


if __name__ == "__main__":
    unittest.main()
