import importlib.util
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from Reporting.service import ReportingService
from api import create_app, live_snapshot
from state import data_lock, state

UTC = timezone.utc


def ms(text):
    return int(datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp() * 1000)


class ReportingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.loc = self.root / 'locations'
        self.usage = self.root / 'usage'
        self.loc.mkdir()
        self.usage.mkdir()
        self.live = None
        self.service = ReportingService(self.loc, self.usage, snapshot=lambda: self.live,
                                        now=lambda: datetime(2026, 10, 5, 12, tzinfo=UTC))

    def location(self, day, content='DE;owner/game;6\n'):
        (self.loc / f'locations-{day}.log').write_text('country;game;n\n' + content)

    def report(self, start='2026-10-01', end='2026-10-05', bucket='day'):
        return self.service.report(ms(start + 'T00:00:00Z'), ms(end + 'T00:00:00Z'), bucket)

    def test_reporter_parity_and_no_early_rounding(self):
        self.location('2026-10-01', 'DE;owner/game;1\nUS;other/game;5\n')
        self.location('2026-10-02', 'DE;owner/game;1\nUS;other/game;1\n')
        self.location('2026-10-03', 'DE;owner/game;1\n')
        result = self.report()
        self.assertEqual(result['summary'][0]['closed_hours'], 1.5)
        self.assertEqual(result['matrix'][-1]['total_hours'], 1.5)
        self.assertEqual(next(r for r in result['matrix'] if r['country'] == 'DE')['owner/game'], .5)
        reporter = Path(__file__).parent.parent / 'Reporter/modules/formatter.py'
        if reporter.exists():
            import pandas
            spec = importlib.util.spec_from_file_location('reporter_formatter', reporter)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            agg = pandas.DataFrame({'country': ['DE', 'US'], 'game': ['owner/game', 'other/game'], 'n': [3, 6]})
            table = module.Formatter(agg, datetime(2026, 10, 1), datetime(2026, 10, 4), '%Y-%m-%d').get_cross_table(agg)
            self.assertEqual(table.loc['All', ('All', '')], 1.5)
            table = table.fillna(0)
            for row in result['matrix']:
                self.assertEqual(round(row['total_hours'], 2), table.loc[row['country'], ('All', '')])
                for game in ['owner/game', 'other/game']:
                    self.assertEqual(round(row[game], 2), table.loc[row['country'], tuple(game.split('/'))])

    def test_midnight_end_is_exclusive(self):
        self.location('2026-10-01')
        self.location('2026-10-02', 'DE;owner/game;60\n')
        self.assertEqual(self.report(end='2026-10-02')['summary'][0]['closed_hours'], 1)
        result = self.service.report(ms('2026-10-01T11:00:00Z'), ms('2026-10-02T00:01:00Z'))
        self.assertEqual(result['summary'][0]['closed_hours'], 11)

    def test_equal_length_previous_period_excludes_today(self):
        for day in range(1, 5):
            self.location(f'2026-10-0{day}', 'DE;owner/game;12\n')
        for day in range(27, 31):
            self.location(f'2026-09-{day}', 'DE;owner/game;6\n')
        self.live = {'locations': [{'game': 'owner/game', 'n': 120}], 'location_period_start': '2026-10-05', 'last_location_measurement': '2026-10-05T11:00:00Z'}
        result = self.service.report(ms('2026-10-01T00:00:00Z'), ms('2026-10-05T12:00:00Z'))
        self.assertEqual(result['summary'][0]['total_hours'], 28)
        self.assertEqual(result['summary'][0]['change_percent'], 100)
        self.assertEqual(result['comparison'][0]['end'], '2026-09-30')
        self.assertEqual(sum(r['hours'] for r in result['countries']), 8)

    def test_missing_days_are_null_and_comparison_unavailable(self):
        self.location('2026-10-01')
        result = self.report()
        self.assertIsNone(result['daily'][1]['hours'])
        self.assertIsNone(result['summary'][0]['change_percent'])
        self.assertEqual(result['summary'][0]['location_days'], 1)

    def test_zero_previous_period_has_no_percentage(self):
        self.location('2026-10-01')
        self.location('2026-09-30', '')
        result = self.report(end='2026-10-02')
        self.assertTrue(result['comparison'][0]['comparable'])
        self.assertIsNone(result['summary'][0]['change_percent'])
        self.assertEqual(result['comparison'][0]['previous_hours'], 0)

    def test_today_without_snapshot_is_missing_not_zero(self):
        result = self.service.report(ms('2026-10-05T00:00:00Z'), ms('2026-10-05T12:00:00Z'))
        self.assertIsNone(result['summary'][0]['total_hours'])
        self.assertFalse(result['provisional'][0]['location_available'])

    def test_unknown_share_is_only_for_closed_country_reports(self):
        self.location('2026-10-04', 'UNKNOWN;owner/game;6\nDE;owner/game;6\n')
        self.live = {'locations': [{'game': 'owner/game', 'n': 60}], 'location_period_start': '2026-10-05', 'last_location_measurement': '2026-10-05T11:00:00Z'}
        result = self.service.report(ms('2026-10-04T00:00:00Z'), ms('2026-10-05T12:00:00Z'))
        self.assertEqual(result['summary'][0]['unknown_percent'], 50)
        self.assertEqual(result['summary'][0]['total_hours'], 12)
        self.assertFalse(result['coverage'][1]['country_report'])

    def test_archive_after_rollover_replaces_provisional_without_double_counting(self):
        self.location('2026-10-05')
        self.service.now = lambda: datetime(2026, 10, 6, 1, tzinfo=UTC)
        self.live = {'locations': [], 'location_period_start': None}
        result = self.service.report(ms('2026-10-05T00:00:00Z'), ms('2026-10-06T01:00:00Z'))
        self.assertEqual(result['summary'][0]['closed_hours'], 1)
        self.assertEqual(result['summary'][0]['total_hours'], 1)
        self.assertIsNone(result['summary'][0]['provisional_hours'])

    def test_today_never_leaks_ips_or_changes_source(self):
        self.live = {'locations': [{'anon-ip': 'PRIVATE', 'game': 'owner/game', 'n': 6}], 'location_period_start': '2026-10-05', 'last_location_measurement': '2026-10-05T11:00:00Z'}
        before = json.dumps(self.live)
        result = self.service.report(ms('2026-10-05T00:00:00Z'), ms('2026-10-05T12:00:00Z'))
        self.assertNotIn('PRIVATE', json.dumps(result))
        self.assertEqual(result['summary'][0]['total_hours'], 1)
        self.assertEqual(result['countries'], [])
        self.assertEqual(json.dumps(self.live), before)
        self.assertEqual(self.service.report(ms('2026-10-05T00:00:00Z'), ms('2026-10-05T12:00:00Z'))['summary'][0]['total_hours'], 1)

    def test_spanning_accumulation_is_not_assigned_to_today(self):
        self.live = {'locations': [{'game': 'owner/game', 'n': 6}], 'location_period_start': '2026-10-04', 'last_location_measurement': '2026-10-05T11:00:00Z'}
        result = self.service.report(ms('2026-10-05T00:00:00Z'), ms('2026-10-05T12:00:00Z'))
        self.assertIsNone(result['summary'][0]['total_hours'])
        self.assertTrue(result['issues'])

    def test_usage_timestamp_is_authoritative_and_invalid_cpu_not_clamped(self):
        (self.usage / 'usage-2026-10-01.log').write_text(',Timestamp,Max_usr,Max_cpu,Max_mem\n0,26-10-02 00:05:00,12,105,40\n0,26-10-02 00:15:00,18,50,45\n')
        result = self.report(start='2026-10-02', end='2026-10-03')
        self.assertEqual(result['summary'][0]['peak_connections'], 18)
        self.assertEqual(result['summary'][0]['peak_host_cpu_percent'], 50)
        self.assertIsNone(result['usage'][0]['host_cpu_percent'])
        self.assertEqual(len(result['issues']), 1)

    def test_bad_location_row_blocks_comparison_and_cache_updates(self):
        self.location('2026-10-01', 'DE;owner/game;6\nUS;owner/game;nan\n')
        self.location('2026-09-30')
        result = self.report(end='2026-10-02')
        self.assertEqual(result['summary'][0]['closed_hours'], 1)
        self.assertFalse(result['comparison'][0]['comparable'])
        self.location('2026-10-01', 'DE;owner/game;12\n')
        result = self.report(end='2026-10-02')
        self.assertEqual(result['summary'][0]['closed_hours'], 2)
        self.assertEqual(result['summary'][0]['change_percent'], 100)

    def test_unreadable_and_empty_files(self):
        (self.loc / 'locations-2026-10-01.log').write_text('bad header\n')
        self.location('2026-10-02', '')
        result = self.report()
        self.assertFalse(result['coverage'][0]['country_report'])
        self.assertTrue(result['coverage'][1]['country_report'])
        self.assertEqual(result['daily'][1]['hours'], 0)
        self.assertTrue(result['issues'])

    def test_calendar_month_week_and_leap_day(self):
        self.service.now = lambda: datetime(2024, 3, 10, tzinfo=UTC)
        self.location('2024-02-29')
        self.location('2024-03-01')
        result = self.report('2024-02-29', '2024-03-02', 'month')
        self.assertEqual([r['period'] for r in result['series']], ['2024-02-01', '2024-03-01'])
        self.assertTrue(all(r['partial'] for r in result['series']))
        result = self.report('2024-02-29', '2024-03-02', 'week')
        self.assertEqual(result['series'][0]['period'], '2024-02-26')
        self.assertEqual(result['series'][0]['time'], '2024-02-29T00:00:00+00:00')
        self.assertEqual(result['series'][0]['hours'], 2)

    def test_api_offline_validation_and_metadata(self):
        self.location('2026-10-01')
        client = create_app(self.service).test_client()
        self.assertEqual(client.get('/reporting/report').status_code, 400)
        self.assertEqual(client.get('/reporting/report?from=0&to=1&bucket=year').status_code, 400)
        self.assertEqual(client.get('/reporting/report?from=5&to=1').status_code, 400)
        self.assertEqual(client.get('/reporting/report?from=-62135596800000&to=0').status_code, 400)
        self.assertEqual(client.get('/reporting/metadata').json['earliest'], '2026-10-01')
        response = client.get('/reporting/report', query_string={'from': ms('2026-10-01T00:00:00Z'), 'to': ms('2026-10-02T00:00:00Z')})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['summary'][0]['closed_hours'], 1)

    def test_actual_snapshot_copies_only_safe_columns(self):
        import pandas
        with data_lock:
            old = state.daily_game_user_log
            state.daily_game_user_log = pandas.DataFrame({'anon-ip': ['PRIVATE'], 'game': ['owner/game'], 'n': [6]})
        try:
            snapshot = live_snapshot()
            self.assertNotIn('PRIVATE', json.dumps(snapshot))
            snapshot['locations'][0]['n'] = 99
            self.assertEqual(state.daily_game_user_log.iloc[0]['n'], 6)
        finally:
            with data_lock:
                state.daily_game_user_log = old


if __name__ == '__main__':
    unittest.main()
