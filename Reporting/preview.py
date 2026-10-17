"""Synthetic offline reporting and metrics for the local Grafana preview."""
import argparse
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from flask import Response

from api import create_app
from Reporting.service import ReportingService


def fixture_app(root, environment):
    root = Path(root)
    locations, usage = root / 'locations', root / 'usage'
    locations.mkdir(parents=True, exist_ok=True)
    usage.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date()
    factor = 1 if environment == 'production' else .2
    games = ['owner/logic', 'owner/numbers', 'other/proofs']
    for offset in range(90, 0, -1):
        day = today - timedelta(days=offset)
        location_rows = ['country;game;n']
        for i, country in enumerate(['DE', 'US', 'GB', 'UNKNOWN']):
            for j, game in enumerate(games):
                n = int(factor * (12 + (90 - offset) * 2 + i * 4 + j * 3))
                location_rows.append(f'{country};{game};{n}')
        (locations / f'locations-{day}.log').write_text('\n'.join(location_rows) + '\n')
        rows = [',Timestamp,Max_usr,Max_cpu,Max_mem']
        for slot in range(144):
            instant = datetime.combine(day, datetime.min.time(), timezone.utc) + timedelta(minutes=10 * slot + 5)
            connections = round(factor * (15 + 10 * (1 + math.sin(slot / 20))))
            rows.append(f'{slot},{instant:%y-%m-%d %H:%M:%S},{connections},{10 + connections * .8:.2f},{30 + connections * .3:.2f}')
        (usage / f'usage-{day}.log').write_text('\n'.join(rows) + '\n')

    def snapshot():
        now = datetime.now(timezone.utc)
        live_rows = []
        for slot in range((now.hour * 60 + now.minute) // 10):
            instant = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=slot * 10 + 5)
            live_rows.append({'Timestamp': instant.isoformat(), 'Max_usr': 20 * factor, 'Max_cpu': 26, 'Max_mem': 36})
        return {'locations': [{'game': game, 'n': round((now.hour * 6 + now.minute / 10) * factor * (j + 1))} for j, game in enumerate(games)],
                'usage': live_rows, 'location_period_start': str(now.date()), 'last_location_measurement': now.isoformat()}

    app = create_app(ReportingService(locations, usage, environment, snapshot=snapshot))

    @app.get('/metrics')
    def metrics():
        now = datetime.now(timezone.utc).timestamp()
        lines = [f'logger_usage_peak_players {20 * factor}', 'logger_usage_peak_cpu_percent 26',
                 'logger_usage_peak_memory_percent 36', f'logger_usage_last_measurement_timestamp_seconds {now - 20}',
                 f'logger_usage_cpu_last_success_timestamp_seconds {now - 20}',
                 'logger_game_cleanup_enabled 0', 'logger_game_lifecycle_games{status="active"} 2',
                 'logger_game_lifecycle_games{status="inactive"} 1', 'logger_game_lifecycle_games{status="deletion_due"} 0']
        for job, age in [('game_activity_refresh', 60), ('location_translation', 3600), ('usage_measurement', 20)]:
            lines.extend([f'logger_job_last_success_timestamp_seconds{{job="{job}"}} {now - age}', f'logger_job_failures_total{{job="{job}"}} 0'])
        # Synthetic CPU counters only; the preview never reads the real host.
        for cpu in (0, 1):
            for mode, fraction in [('idle', .74), ('user', .20), ('iowait', .04), ('steal', .02)]:
                lines.append(f'node_cpu_seconds_total{{cpu="{cpu}",mode="{mode}"}} {now * fraction}')
        for i, game in enumerate(games):
            status = 'active' if i < 2 else 'inactive'
            lines.extend([f'logger_game_lifecycle_info{{game="{game}",status="{status}"}} 1',
                          f'logger_game_lifecycle_last_played_timestamp_seconds{{game="{game}"}} {now - (i + 1) * 86400}',
                          f'logger_game_lifecycle_inactive_at_timestamp_seconds{{game="{game}"}} {now + (5 - i) * 86400}',
                          f'logger_game_lifecycle_deletion_due_at_timestamp_seconds{{game="{game}"}} {now + (35 - i) * 86400}'])
        return Response('\n'.join(lines) + '\n', mimetype='text/plain')
    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=18877)
    parser.add_argument('--environment', choices=['production', 'development'], default='production')
    args = parser.parse_args()
    with TemporaryDirectory(prefix='logger-demo-data-') as root:
        fixture_app(root, args.environment).run(host='127.0.0.1', port=args.port)
