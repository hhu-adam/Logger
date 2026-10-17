"""Read existing UTC daily archives without changing their contents."""

import csv
import math
import re
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from threading import RLock

UTC = timezone.utc
LOCATION_PATTERN = re.compile(r"locations-(\d{4}-\d{2}-\d{2})\.log$")
USAGE_PATTERN = re.compile(r"usage-(\d{4}-\d{2}-\d{2})\.log$")


def parse_timestamp(value):
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = datetime.strptime(text, "%y-%m-%d %H:%M:%S")
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def number(value, maximum=None, integer=False):
    result = float(value)
    if not math.isfinite(result) or result < 0 or (maximum is not None and result > maximum):
        raise ValueError("outside valid range")
    if integer and not result.is_integer():
        raise ValueError("observation count is not an integer")
    return result


def day_range(start, end):
    return [start + timedelta(days=i) for i in range(max(0, (end - start).days + 1))]


def bucket_start(day, bucket):
    if bucket == "week":
        return day - timedelta(days=day.weekday())
    return day.replace(day=1) if bucket == "month" else day


class ReportingService:
    def __init__(self, location_dir, usage_dir, environment="production", snapshot=None, now=None):
        self.location_dir = Path(location_dir)
        self.usage_dir = Path(usage_dir)
        self.environment = environment
        self.snapshot = snapshot
        self.now = now or (lambda: datetime.now(UTC))
        self._cache = {}
        self._lock = RLock()

    def _files(self, directory, pattern):
        result = []
        for path in sorted(directory.glob("*.log")):
            match = pattern.fullmatch(path.name)
            if match:
                try:
                    result.append((datetime.strptime(match[1], "%Y-%m-%d").date(), path))
                except ValueError:
                    pass
        return result

    def _read(self, path, kind):
        # Failed/transient reads are not cached, so a later refresh can recover.
        try:
            stat = path.stat()
            key = (str(path), stat.st_mtime_ns, stat.st_size)
            with self._lock:
                if key in self._cache:
                    return self._cache[key]
            rows, issues = [], []
            with path.open(newline="", encoding="utf-8-sig") as stream:
                reader = csv.DictReader(stream, delimiter=";" if kind == "location" else ",")
                expected = {"country", "game", "n"} if kind == "location" else {"Timestamp", "Max_usr", "Max_cpu", "Max_mem"}
                if not expected.issubset(reader.fieldnames or []):
                    return [], [{"file": path.name, "reason": "missing CSV columns"}], False
                for line, row in enumerate(reader, 2):
                    try:
                        if kind == "location":
                            country, game = row["country"].strip(), row["game"].strip()
                            if not country or not game:
                                raise ValueError("empty country or game")
                            rows.append({"country": country, "game": game, "observations": number(row["n"], integer=True)})
                        else:
                            parsed = {"timestamp": parse_timestamp(row["Timestamp"])}
                            for field, maximum in (("Max_usr", None), ("Max_cpu", 100), ("Max_mem", 100)):
                                try:
                                    parsed[field] = number(row[field], maximum)
                                except (ValueError, TypeError):
                                    parsed[field] = None
                                    issues.append({"file": path.name, "line": line, "reason": f"invalid {field}"})
                            rows.append(parsed)
                    except (ValueError, TypeError, AttributeError, KeyError):
                        issues.append({"file": path.name, "line": line, "reason": "invalid row or timestamp"})
            after = path.stat()
            if (after.st_mtime_ns, after.st_size) != (stat.st_mtime_ns, stat.st_size):
                return [], [{"file": path.name, "reason": "file changed during read; retry on refresh"}], False
            result = rows, issues, True
            with self._lock:
                self._cache = {k: v for k, v in self._cache.items() if k[0] != str(path)}
                self._cache[key] = result
            return result
        except (OSError, UnicodeError, csv.Error):
            return [], [{"file": path.name, "reason": "unreadable CSV"}], False

    def metadata(self):
        dates = [day for directory, pattern in ((self.location_dir, LOCATION_PATTERN), (self.usage_dir, USAGE_PATTERN)) for day, _ in self._files(directory, pattern)]
        live = self.snapshot() if self.snapshot else None
        return {"environment": self.environment, "timezone": "UTC", "earliest": str(min(dates)) if dates else None,
                "latest": str(max(dates)) if dates else None, "live_available": bool(live and (live.get("last_location_measurement") or live.get("usage"))), "today": str(self.now().date())}

    def report(self, from_ms, to_ms, bucket="day"):
        if bucket not in {"day", "week", "month"}:
            raise ValueError("bucket must be day, week or month")
        try:
            start_dt = datetime.fromtimestamp(int(from_ms) / 1000, UTC)
            end_dt = datetime.fromtimestamp(int(to_ms) / 1000, UTC)
        except (ValueError, TypeError, OverflowError, OSError):
            raise ValueError("from and to must be Unix milliseconds") from None
        if start_dt >= end_dt:
            raise ValueError("from must be before to")
        now = self.now()
        today = now.date()
        start = start_dt.date()
        end = min((end_dt - timedelta(microseconds=1)).date(), today)
        if (end - start).days > 36600:
            raise ValueError("report range must not exceed 100 years")
        selected = day_range(start, end)
        closed = [day for day in selected if day < today]
        try:
            prev_days = day_range(start - timedelta(days=len(closed)), start - timedelta(days=1)) if closed else []
        except OverflowError:
            raise ValueError("comparison period is outside supported calendar dates") from None
        needed = set(selected + prev_days)
        locations, usage, issues = defaultdict(list), defaultdict(list), []
        loc_present, usage_present = set(), set()
        for day, path in self._files(self.location_dir, LOCATION_PATTERN):
            if day in needed and day < today:
                rows, errors, readable = self._read(path, "location")
                issues.extend(errors)
                if readable and (not errors or rows):
                    loc_present.add(day)
                    locations[day].extend(rows)
        # Usage belongs to its timestamp's UTC date, not the archive filename.
        for _, path in self._files(self.usage_dir, USAGE_PATTERN):
            rows, errors, readable = self._read(path, "usage")
            relevant = [row for row in rows if row["timestamp"].date() in needed and row["timestamp"].date() < today]
            if relevant or any(str(day) in path.name for day in needed):
                issues.extend(errors)
            for row in relevant:
                day = row["timestamp"].date()
                usage[day].append(row)
                usage_present.add(day)
        live = self.snapshot() if today in selected and self.snapshot else None
        provisional = None
        if today in selected:
            provisional = {"date": str(today), "observations": None, "hours": None, "location_available": False, "usage_available": False, "last_location_measurement": None}
            if live:
                if live.get("location_period_start") == str(today) and live.get("last_location_measurement"):
                    total = 0
                    for row in live.get("locations", []):
                        try:
                            value = number(row["n"], integer=True)
                            game = str(row["game"])
                            # Never copy or return the anon-ip field.
                            locations[today].append({"game": game, "observations": value})
                            total += value
                        except (KeyError, ValueError, TypeError):
                            issues.append({"file": "live", "reason": "invalid location observation"})
                    provisional.update(observations=total, hours=total / 6, location_available=True, last_location_measurement=live["last_location_measurement"])
                elif live.get("location_period_start"):
                    issues.append({"file": "live", "reason": "location accumulation spans earlier days; excluded from today"})
                for row in live.get("usage", []):
                    try:
                        timestamp = parse_timestamp(row["Timestamp"])
                        if timestamp.date() != today:
                            continue
                        parsed = {"timestamp": timestamp}
                        for field, maximum in (("Max_usr", None), ("Max_cpu", 100), ("Max_mem", 100)):
                            try:
                                parsed[field] = number(row[field], maximum)
                            except (ValueError, TypeError):
                                parsed[field] = None
                                issues.append({"file": "live", "reason": f"invalid {field}"})
                        usage[today].append(parsed)
                    except (KeyError, ValueError, TypeError):
                        issues.append({"file": "live", "reason": "invalid usage timestamp"})
                provisional["usage_available"] = bool(usage[today])

        def totals(days):
            return sum(row["observations"] for day in days for row in locations[day])

        def maximum(days, field):
            values = [row[field] for day in days for row in usage[day] if row[field] is not None]
            return max(values) if values else None

        current_observations = totals(closed)
        previous_observations = totals(prev_days)
        invalid_location_files = {issue["file"] for issue in issues if issue["file"].startswith("locations-")}

        def reports_available(days):
            return bool(days) and all(day in loc_present and f"locations-{day}.log" not in invalid_location_files for day in days)

        current_complete = reports_available(closed)
        previous_complete = reports_available(prev_days)
        comparable = current_complete and previous_complete
        percent = 100 * (current_observations - previous_observations) / previous_observations if comparable and previous_observations else None
        country_totals, game_totals, cells = defaultdict(float), defaultdict(float), defaultdict(float)
        for day in selected:
            for row in locations[day]:
                game_totals[row["game"]] += row["observations"]
                if day < today:
                    country_totals[row["country"]] += row["observations"]
                    cells[row["country"], row["game"]] += row["observations"]
        ranked = lambda values, field: [{field: name, "observations": count, "hours": count / 6} for name, count in sorted(values.items(), key=lambda item: (-item[1], item[0]))]
        closed_games = defaultdict(float)
        for (_, game), value in cells.items():
            closed_games[game] += value
        matrix = [{"country": row["country"], "total_hours": row["hours"], **{game: cells[row["country"], game] / 6 for game in closed_games}} for row in ranked(country_totals, "country")]
        if matrix:
            matrix.append({"country": "All", "total_hours": current_observations / 6, **{game: n / 6 for game, n in closed_games.items()}})
        groups = defaultdict(list)
        daily = []
        for day in selected:
            known = day in loc_present or (day == today and provisional["location_available"])
            daily.append({"date": str(day), "time": datetime.combine(day, time(), UTC).isoformat(), "hours": totals([day]) / 6 if known else None,
                          "location_available": known, "country_report_available": day < today and day in loc_present,
                          "usage_available": day in usage_present or bool(usage[day]), "provisional": day == today,
                          "peak_connections": maximum([day], "Max_usr"), "peak_host_cpu_percent": maximum([day], "Max_cpu"), "peak_host_ram_percent": maximum([day], "Max_mem")})
            groups[bucket_start(day, bucket)].append(day)
        series = []
        daily_by_date = {row["date"]: row for row in daily}
        for first, days in sorted(groups.items()):
            entries = [daily_by_date[str(day)] for day in days]
            available = [row["hours"] for row in entries if row["hours"] is not None]
            last = first if bucket == "day" else first + timedelta(days=6) if bucket == "week" else (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            # Keep partial buckets visible inside Grafana's requested axis;
            # `period` retains the actual calendar bucket start.
            plot_time = max(datetime.combine(first, time(), UTC), start_dt)
            series.append({"time": plot_time.isoformat(), "period": str(first), "hours": sum(available) if available else None,
                           "partial": days[0] != first or days[-1] != last or any(row["provisional"] for row in entries),
                           "days_with_locations": sum(row["location_available"] for row in entries), "selected_days": len(days),
                           "peak_connections": maximum(days, "Max_usr"), "peak_host_cpu_percent": maximum(days, "Max_cpu"), "peak_host_ram_percent": maximum(days, "Max_mem")})
        summary = {"closed_hours": current_observations / 6 if loc_present.intersection(closed) else None,
                   "provisional_hours": provisional["hours"] if provisional else None,
                   "total_hours": totals(selected) / 6 if game_totals or loc_present.intersection(selected) or (provisional and provisional["location_available"]) else None,
                   "change_percent": percent, "unknown_percent": 100 * country_totals.get("UNKNOWN", 0) / current_observations if current_observations else None,
                   "location_days": len(loc_present.intersection(closed)), "usage_days": len(usage_present.intersection(closed)), "closed_days": len(closed),
                   "peak_connections": maximum(selected, "Max_usr"), "peak_host_cpu_percent": maximum(selected, "Max_cpu"), "peak_host_ram_percent": maximum(selected, "Max_mem")}
        detail = [{"time": row["timestamp"].isoformat(), "connections": row["Max_usr"], "host_cpu_percent": row["Max_cpu"], "host_ram_percent": row["Max_mem"]} for day in selected for row in usage[day]]
        return {"environment": self.environment, "timezone": "UTC", "start": str(start), "end": str(end) if selected else None,
                "bucket": bucket, "summary": [summary], "series": series, "daily": daily,
                "countries": ranked(country_totals, "country"), "games": ranked(game_totals, "game"), "matrix": matrix,
                "usage": sorted(detail, key=lambda row: row["time"]), "provisional": [provisional] if provisional else [],
                "comparison": [{"start": str(prev_days[0]) if prev_days else None, "end": str(prev_days[-1]) if prev_days else None,
                                "current_hours": current_observations / 6 if loc_present.intersection(closed) else None,
                                "previous_hours": previous_observations / 6 if loc_present.intersection(prev_days) else None, "comparable": comparable, "change_percent": percent}],
                "coverage": [{"date": row["date"], "game_observations": row["location_available"], "country_report": row["country_report_available"],
                              "usage": row["usage_available"], "provisional": row["provisional"]} for row in daily],
                "issues": issues}
