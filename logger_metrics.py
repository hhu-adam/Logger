"""Prometheus metrics exported by the Lean4Game Logger service.

The metrics deliberately contain aggregated operational data only.  In
particular, anonymized IP addresses are never exported.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from time import monotonic, time
from typing import Any, Iterator

import pandas
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, REGISTRY, start_http_server


def _timestamp_seconds(value: str | None) -> float | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


class LoggerMetrics:
    """Keeps Logger's Prometheus metrics in sync with completed jobs."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.registry = registry
        self.service_up = Gauge(
            "logger_service_up",
            "Whether the Logger process has started successfully.",
            registry=registry,
        )
        self.job_last_run = Gauge(
            "logger_job_last_run_timestamp_seconds",
            "Unix timestamp of the latest attempt for a Logger job.",
            ["job"],
            registry=registry,
        )
        self.job_last_success = Gauge(
            "logger_job_last_success_timestamp_seconds",
            "Unix timestamp of the latest successful Logger job.",
            ["job"],
            registry=registry,
        )
        self.job_failures = Counter(
            "logger_job_failures_total",
            "Number of failed Logger job attempts.",
            ["job"],
            registry=registry,
        )
        self.job_duration = Histogram(
            "logger_job_duration_seconds",
            "Duration of Logger job attempts.",
            ["job"],
            registry=registry,
        )

        self.usage_peak_players = Gauge(
            "logger_usage_peak_players",
            "Peak concurrently active Lean4Game players in the current measurement window.",
            registry=registry,
        )
        self.usage_peak_cpu = Gauge(
            "logger_usage_peak_cpu_percent",
            "Peak host CPU usage in the current measurement window, as a percentage.",
            registry=registry,
        )
        self.usage_peak_memory = Gauge(
            "logger_usage_peak_memory_percent",
            "Peak host RAM usage in the current measurement window, as a percentage.",
            registry=registry,
        )
        self.usage_measurement = Gauge(
            "logger_usage_last_measurement_timestamp_seconds",
            "Unix timestamp of the latest Logger usage measurement.",
            registry=registry,
        )

        self.lifecycle_counts = Gauge(
            "logger_game_lifecycle_games",
            "Number of games in each lifecycle state.",
            ["status"],
            registry=registry,
        )
        self.lifecycle_info = Gauge(
            "logger_game_lifecycle_info",
            "Current lifecycle state for an installed game.",
            ["game", "status"],
            registry=registry,
        )
        self.lifecycle_last_activity = Gauge(
            "logger_game_lifecycle_last_activity_timestamp_seconds",
            "Most recent activity timestamp for an installed game.",
            ["game"],
            registry=registry,
        )
        self.lifecycle_inactive_at = Gauge(
            "logger_game_lifecycle_inactive_at_timestamp_seconds",
            "Timestamp at which an installed game becomes inactive.",
            ["game"],
            registry=registry,
        )
        self.lifecycle_deletion_due_at = Gauge(
            "logger_game_lifecycle_deletion_due_at_timestamp_seconds",
            "Timestamp at which an installed game becomes eligible for cleanup.",
            ["game"],
            registry=registry,
        )
        self.cleanup_enabled = Gauge(
            "logger_game_cleanup_enabled",
            "Whether automatic game cleanup is enabled (1) or preview-only (0).",
            registry=registry,
        )
        self.cleanup_candidates = Gauge(
            "logger_game_cleanup_candidates",
            "Cleanup candidates grouped by resulting action.",
            ["action"],
            registry=registry,
        )
        self.cleanup_skips = Gauge(
            "logger_game_cleanup_skipped_games",
            "Skipped cleanup candidates grouped by reason.",
            ["reason"],
            registry=registry,
        )
        self.cleanup_expired_trash = Gauge(
            "logger_game_cleanup_expired_trash_entries",
            "Number of expired trash entries found during the latest cleanup run.",
            registry=registry,
        )
        self.location_usage = Gauge(
            "logger_location_game_observations",
            "Aggregated daily session observations by country and game.",
            ["country", "game"],
            registry=registry,
        )
        self._lifecycle_labels: set[tuple[str, str]] = set()
        self._lifecycle_games: set[str] = set()
        self._location_labels: set[tuple[str, str]] = set()
        self._cleanup_skip_reasons: set[str] = set()

    def start(self, host: str, port: int) -> None:
        """Start a dedicated, read-only Prometheus HTTP endpoint."""
        start_http_server(port, addr=host, registry=self.registry)
        self.service_up.set(1)

    @contextmanager
    def observe_job(self, name: str) -> Iterator[None]:
        """Record a job attempt while preserving the job's existing behaviour."""
        self.job_last_run.labels(name).set(time())
        started = monotonic()
        try:
            yield
        except Exception:
            self.job_failures.labels(name).inc()
            raise
        else:
            self.job_last_success.labels(name).set(time())
        finally:
            self.job_duration.labels(name).observe(monotonic() - started)

    def record_usage(self, measurement: pandas.DataFrame) -> None:
        """Publish a single-row UsageMeter result."""
        if measurement.empty:
            return
        latest = measurement.iloc[-1]
        self.usage_peak_players.set(float(latest["Max_usr"]))
        self.usage_peak_cpu.set(float(latest["Max_cpu"]))
        self.usage_peak_memory.set(float(latest["Max_mem"]))
        timestamp = _timestamp_seconds(str(latest["Timestamp"]))
        self.usage_measurement.set(timestamp if timestamp is not None else time())

    def record_activity_report(self, report: dict[str, Any]) -> None:
        """Publish the current state and timeline of each game."""
        games = report.get("games", {})
        current_labels: set[tuple[str, str]] = set()
        current_games: set[str] = set()
        counts = {"active": 0, "inactive": 0, "deletion_due": 0}

        for game, activity in games.items():
            status = str(activity.get("status", "unknown"))
            game = str(game)
            current_labels.add((game, status))
            current_games.add(game)
            self.lifecycle_info.labels(game, status).set(1)
            if status in counts:
                counts[status] += 1
            for value, metric in (
                (activity.get("lastActivityAt"), self.lifecycle_last_activity),
                (activity.get("inactiveAt"), self.lifecycle_inactive_at),
                (activity.get("deletionDueAt"), self.lifecycle_deletion_due_at),
            ):
                timestamp = _timestamp_seconds(value)
                if timestamp is not None:
                    metric.labels(game).set(timestamp)

        for game, status in self._lifecycle_labels - current_labels:
            self.lifecycle_info.remove(game, status)
        for game in self._lifecycle_games - current_games:
            self.lifecycle_last_activity.remove(game)
            self.lifecycle_inactive_at.remove(game)
            self.lifecycle_deletion_due_at.remove(game)
        for status, count in counts.items():
            self.lifecycle_counts.labels(status).set(count)
        self._lifecycle_labels = current_labels
        self._lifecycle_games = current_games

    def record_cleanup_report(self, report: dict[str, Any]) -> None:
        """Publish the result of one automatic cleanup pass."""
        self.cleanup_enabled.set(1 if report.get("cleanupEnabled") else 0)
        actions: dict[str, int] = {}
        skip_reasons: dict[str, int] = {}
        for candidate in report.get("candidates", []):
            action = str(candidate.get("action", "unknown"))
            actions[action] = actions.get(action, 0) + 1
            if action == "skipped":
                reason = str(candidate.get("reason", "unknown"))
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        for action in ("eligible", "moved_to_trash", "skipped"):
            self.cleanup_candidates.labels(action).set(actions.get(action, 0))
        for reason, count in skip_reasons.items():
            self.cleanup_skips.labels(reason).set(count)
        for reason in self._cleanup_skip_reasons - set(skip_reasons):
            self.cleanup_skips.remove(reason)
        self._cleanup_skip_reasons = set(skip_reasons)
        self.cleanup_expired_trash.set(len(report.get("expiredTrash", [])))

    def record_location_usage(self, locations: pandas.DataFrame) -> None:
        """Publish daily country/game aggregates, never individual IP addresses."""
        current_labels: set[tuple[str, str]] = set()
        for row in locations.itertuples(index=False):
            country = str(row.country)
            game = str(row.game)
            current_labels.add((country, game))
            self.location_usage.labels(country, game).set(float(row.n))
        for country, game in self._location_labels - current_labels:
            self.location_usage.remove(country, game)
        self._location_labels = current_labels


metrics = LoggerMetrics()
