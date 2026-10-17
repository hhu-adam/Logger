"""CPU peaks calculated from timestamped node-exporter counters in Prometheus."""

import json
import logging
import math

from prometheus_api_client.prometheus_connect import PrometheusConnect


log = logging.getLogger(__name__)


class CpuMeter:
    def __init__(
        self,
        prometheus_connection: PrometheusConnect,
        *,
        job: str = "",
        instance: str = "",
    ) -> None:
        self.prom_con = prometheus_connection
        matchers = []
        for label, value in (("job", job), ("instance", instance)):
            if value:
                matchers.append(f"{label}={json.dumps(value)}")
        counter = "node_cpu_seconds_total{" + ",".join(['mode="idle"', *matchers]) + "}"
        total_counter = "node_cpu_seconds_total{" + ",".join([
            'mode!="guest"', 'mode!="guest_nice"', *matchers,
        ]) + "}"

        # Apply rate before averaging so counter resets are handled per core.
        # Normalize by total CPU time from the same samples: rate extrapolation
        # near a missed scrape must not turn a lower idle rate into a false peak.
        # Guest time is excluded because it is already counted in user/nice.
        # A fresh query timestamp alone does not prove the scraped data is fresh.
        idle_fraction = f"""((
            rate({counter}[1m]) / ignoring (mode)
            sum without (mode) (rate({total_counter}[1m]))
        ) >= 0 <= 1)"""
        current = f"""(
            100 * (1 - avg by (job, instance) ({idle_fraction}))
            and on (job, instance)
            (min by (job, instance) (timestamp({counter})) > time() - 45)
            and on (job, instance)
            (min by (job, instance) (count_over_time({counter}[1m])) >= 3)
            and on (job, instance)
            (min by (job, instance) (timestamp({counter} offset 1m)) > time() - 105)
            and on (job, instance)
            (count by (job, instance) ({idle_fraction})
             == count by (job, instance) ({counter}))
        )"""
        # Require all 40 evaluations in the ten-minute window. Missing history
        # must not turn a partial maximum into an apparently complete peak.
        self.peak_query = f"""
        max_over_time({current}[10m:15s])
        and on (job, instance) (count_over_time({current}[10m:15s]) >= 40)
        and on (job, instance) {current}
        and on () (count(count by (job, instance) ({counter})) == 1)
        """

    def get_max_cpu_usage_over_last_ten_min(self) -> float:
        """Return the peak one-minute non-idle CPU percentage, or NaN if unknown."""
        try:
            results = self.prom_con.custom_query(self.peak_query)
        except Exception:
            log.exception("CPU measurement unavailable: Prometheus query failed")
            return math.nan

        if len(results) != 1:
            log.warning(
                "CPU measurement unavailable: expected one target, got %d. "
                "Check source coverage and PROMETHEUS_NODE_JOB/INSTANCE.",
                len(results),
            )
            return math.nan
        try:
            peak = float(results[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            log.warning("CPU measurement unavailable: invalid Prometheus response")
            return math.nan
        if not math.isfinite(peak) or not 0 <= peak <= 100:
            log.warning("CPU measurement unavailable: invalid percentage %s", peak)
            return math.nan
        return peak
