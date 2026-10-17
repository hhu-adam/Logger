"""CPU collector tests, including the real PromQL engine when promtool is set."""

import json
import math
import os
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from Usage.measurement.cpu_meter import CpuMeter


class CpuMeterTest(unittest.TestCase):
    def setUp(self):
        self.prometheus = Mock()
        self.meter = CpuMeter(self.prometheus)

    def test_repeated_reads_do_not_depend_on_a_local_counter_baseline(self):
        self.prometheus.custom_query.return_value = [{"value": [1000, "10"]}]
        self.assertEqual(self.meter.get_max_cpu_usage_over_last_ten_min(), 10)
        self.assertEqual(self.meter.get_max_cpu_usage_over_last_ten_min(), 10)
        # A Logger restart can immediately query existing Prometheus history.
        self.assertEqual(CpuMeter(self.prometheus).get_max_cpu_usage_over_last_ten_min(), 10)

    def test_unavailable_and_ambiguous_targets_do_not_become_zero(self):
        for results in ([], [{"value": [1, "10"]}] * 2):
            with self.subTest(results=results), self.assertLogs(level="WARNING"):
                self.prometheus.custom_query.return_value = results
                self.assertTrue(math.isnan(self.meter.get_max_cpu_usage_over_last_ten_min()))

    def test_invalid_measurements_are_not_clamped(self):
        for value in ("NaN", "+Inf", "-Inf", "-1", "100.1", "invalid", None):
            with self.subTest(value=value), self.assertLogs(level="WARNING"):
                self.prometheus.custom_query.return_value = [{"value": [1, value]}]
                self.assertTrue(math.isnan(self.meter.get_max_cpu_usage_over_last_ten_min()))

    def test_true_zero_and_full_usage_are_valid(self):
        for value in (0, 100):
            self.prometheus.custom_query.return_value = [{"value": [1, str(value)]}]
            self.assertEqual(self.meter.get_max_cpu_usage_over_last_ten_min(), value)

    def test_query_failure_marks_cpu_unknown(self):
        self.prometheus.custom_query.side_effect = ConnectionError("offline")
        with self.assertLogs(level="ERROR"):
            self.assertTrue(math.isnan(self.meter.get_max_cpu_usage_over_last_ten_min()))

    def test_source_labels_are_escaped(self):
        job, instance = 'node"exporter', 'vm\\host:9100'
        query = CpuMeter(self.prometheus, job=job, instance=instance).peak_query
        self.assertIn('job=' + json.dumps(job), query)
        self.assertIn('instance=' + json.dumps(instance), query)


PROMTOOL = os.getenv("PROMTOOL") or shutil.which("promtool")


@unittest.skipUnless(PROMTOOL, "Set PROMTOOL or install promtool to test actual PromQL")
class CpuPromQLTest(unittest.TestCase):
    def run_case(self, series, *, expected=10, at="15m", job="", instance="", expression=None, labels=None):
        query = expression or CpuMeter(Mock(), job=job, instance=instance).peak_query
        samples = [] if expected is None else [
            {"labels": labels or '{job="node",instance="vm:9100"}', "value": expected}
        ]
        document = {
            "fuzzy_compare": True,
            "evaluation_interval": "15s",
            "tests": [{
                "interval": "15s", "input_series": series,
                "promql_expr_test": [{"expr": query, "eval_time": at, "exp_samples": samples}],
            }],
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "cpu.test.json"
            path.write_text(json.dumps(document))  # JSON is valid YAML.
            result = subprocess.run(
                [PROMTOOL, "test", "rules", str(path)],
                capture_output=True, text=True, timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    @staticmethod
    def core(values="0+13.5x80", cpu="0", job="node", instance="vm:9100", mode="idle"):
        return {
            "series": f'node_cpu_seconds_total{{environment="production",job="{job}",instance="{instance}",cpu="{cpu}",mode="{mode}"}}',
            "values": values,
        }

    def test_steady_load_averages_cores(self):
        self.run_case([
            self.core(), self.core("0+1.5x80", mode="user"),
            self.core("0+6x80", cpu="1"), self.core("0+9x80", cpu="1", mode="user"),
        ], expected=35)

    def test_single_missed_scrape_does_not_create_a_peak(self):
        values = " ".join("_" if i == 40 else str(i * 13.5) for i in range(81))
        busy = " ".join("_" if i == 40 else str(i * 1.5) for i in range(81))
        self.run_case([self.core(values), self.core(busy, mode="user")])

    def test_counter_resets_are_handled_before_averaging(self):
        def reset(increment, reset_at):
            return " ".join(str((i if i < reset_at else i - reset_at + 1) * increment) for i in range(81))
        self.run_case([
            self.core(reset(13.5, 44)),
            self.core(reset(1.5, 44), mode="user"),
            self.core(reset(6, 50), cpu="1"),
            self.core(reset(9, 50), cpu="1", mode="user"),
        ], expected=35)

    def test_a_real_one_minute_peak_is_retained_after_load_falls(self):
        idle, busy = [1000], [1000]
        for i in range(1, 81):
            idle_increment = 3 if 40 <= i <= 43 else 13.5
            idle.append(idle[-1] + idle_increment)
            busy.append(busy[-1] + 15 - idle_increment)
        self.run_case([
            self.core(" ".join(map(str, idle))),
            self.core(" ".join(map(str, busy)), mode="user"),
        ], expected=80)

    def test_source_outage_is_not_an_old_peak(self):
        self.run_case([self.core("0+13.5x60"), self.core("0+1.5x60", mode="user")], at="16m", expected=None)

    def test_partial_window_is_unavailable(self):
        self.run_case([self.core(), self.core("0+1.5x80", mode="user")], at="5m", expected=None)

    def test_gap_inside_window_makes_peak_unavailable(self):
        values = " ".join("_" if 30 <= i <= 38 else str(i * 13.5) for i in range(81))
        busy = " ".join("_" if 30 <= i <= 38 else str(i * 1.5) for i in range(81))
        self.run_case([self.core(values), self.core(busy, mode="user")], expected=None)

    def test_measurement_recovers_when_the_gap_leaves_the_window(self):
        idle = " ".join("_" if 30 <= i <= 38 else str(i * 13.5) for i in range(121))
        busy = " ".join("_" if 30 <= i <= 38 else str(i * 1.5) for i in range(121))
        self.run_case([self.core(idle), self.core(busy, mode="user")], at="25m")

    def test_slow_scrapes_do_not_produce_a_misleading_peak(self):
        values = " ".join(str(i * 13.5) if i % 4 == 0 else "_" for i in range(81))
        busy = " ".join(str(i * 1.5) if i % 4 == 0 else "_" for i in range(81))
        self.run_case([self.core(values), self.core(busy, mode="user")], expected=None)

    def test_configured_source_excludes_other_hosts(self):
        self.run_case([
            self.core(), self.core("0+1.5x80", mode="user"),
            self.core("0+3x80", instance="other:9100"),
            self.core("0+12x80", instance="other:9100", mode="user"),
        ], job="node", instance="vm:9100")

    def test_real_zero_and_full_usage(self):
        self.run_case([self.core("1000+15x80"), self.core("0+0x80", mode="user")], expected=0)
        self.run_case([self.core("1000+0x80"), self.core("0+15x80", mode="user")], expected=100)

    def test_overlapping_guest_time_is_not_counted_twice(self):
        self.run_case([
            self.core(), self.core("0+1.5x80", mode="user"),
            self.core("0+1.5x80", mode="guest"),
            self.core("0+1.5x80", mode="guest_nice"),
        ])

    def test_frozen_counters_are_unknown_instead_of_an_old_peak(self):
        idle = " ".join(str(min(i, 57) * 13.5) for i in range(81))
        busy = " ".join(str(min(i, 57) * 1.5) for i in range(81))
        self.run_case([self.core(idle), self.core(busy, mode="user")], expected=None)

    def test_one_invalid_core_does_not_turn_into_a_partial_vm_average(self):
        self.run_case([
            self.core(), self.core("0+1.5x80", mode="user"),
            self.core("1000+0x80", cpu="1"), self.core("1000+0x80", cpu="1", mode="user"),
        ], expected=None)

    def test_an_ambiguous_target_with_short_history_is_not_silently_ignored(self):
        self.run_case([
            self.core(), self.core("0+1.5x80", mode="user"),
            self.core(" ".join(["_"] * 58 + [str(i * 3) for i in range(23)]), instance="other:9100"),
            self.core(" ".join(["_"] * 58 + [str(i * 12) for i in range(23)]), instance="other:9100", mode="user"),
        ], expected=None)

    def test_dashboard_mode_queries_use_the_same_cpu_percentages(self):
        dashboard = json.loads((Path(__file__).parent / "grafana/lean4game-logger-usage.json").read_text())
        series = [self.core("1000+9x80"), self.core("1000+3x80", mode="user"),
                  self.core("1000+1.5x80", mode="iowait"), self.core("1000+1.5x80", mode="steal")]
        for panel in dashboard["panels"]:
            if panel["id"] not in (143, 144):
                continue
            for target in panel["targets"]:
                if "node_cpu_seconds_total" not in target["expr"]:
                    continue
                with self.subTest(panel=panel["id"], target=target["refId"]):
                    self.run_case(series, expected=40 if panel["id"] == 143 else 10,
                                  expression=target["expr"].replace("$environment", "production").replace("$node_instance", "vm:9100"))

    def test_dashboard_expires_frozen_cpu_and_preserves_older_metric_history(self):
        dashboard = json.loads((Path(__file__).parent / "grafana/lean4game-logger-usage.json").read_text())
        expression = next(p for p in dashboard["panels"] if p["id"] == 6)["targets"][0]["expr"].replace("$environment", "production")
        series = [
            {"series": 'logger_usage_peak_cpu_percent{environment="production"}', "values": "10+0x120"},
            {"series": 'logger_usage_last_measurement_timestamp_seconds{environment="production"}', "values": "600+0x120"},
        ]
        labels = 'logger_usage_peak_cpu_percent{environment="production"}'
        self.run_case(series, expected=10, expression=expression, labels=labels)
        self.run_case(series, at="21m", expected=None, expression=expression)


if __name__ == "__main__":
    unittest.main()
