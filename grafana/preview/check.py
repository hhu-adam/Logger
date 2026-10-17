"""Check actual provisioned dashboards through Grafana's backend query API."""
import argparse
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen


def check():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:13000')
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    errors = []
    checked = 0
    for source in Path(__file__).resolve().parent.parent.glob('lean4game-logger-*.json'):
        dashboard = json.loads(source.read_text())
        with urlopen(args.url + '/api/dashboards/uid/' + dashboard['uid']) as response:
            saved = json.load(response)['dashboard']
        for environment in ['production', 'development']:
            for panel in saved['panels']:
                if not panel.get('targets'):
                    continue
                queries = copy.deepcopy(panel['targets'])
                infinity = panel['datasource']['type'] == 'yesoreyeram-infinity-datasource'
                for query in queries:
                    query['datasource']['uid'] = 'logger-reports-' + environment if infinity else 'logger-preview-prometheus'
                    if infinity:
                        query['url'] = query['url'].replace('${bucket}', 'day')
                    else:
                        query['expr'] = query['expr'].replace('$environment', environment).replace('$__range', '5m')
                        query['expr'] = query['expr'].replace('$node_instance', '127.0.0.1:18877' if environment == 'production' else '127.0.0.1:18878')
                        query['range'] = not query.get('instant', False)
                        query.setdefault('format', 'time_series')
                        query['intervalMs'] = 5000
                        query['maxDataPoints'] = 1000
                # Demo Prometheus history begins at startup. A whole-month
                # range can select a coarse step whose last point predates it.
                query_start = start if infinity else now - timedelta(minutes=5)
                payload = {'from': str(int(query_start.timestamp() * 1000)), 'to': str(int(now.timestamp() * 1000)), 'queries': queries}
                request = Request(args.url + '/api/ds/query', data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
                with urlopen(request, timeout=30) as response:
                    data = json.load(response)
                for ref, result in data['results'].items():
                    if result.get('error'):
                        errors.append(f'{source.name} {panel["id"]} {environment} {ref}: {result["error"]}')
                    elif panel['id'] != 129 and not any(frame.get('schema', {}).get('fields') for frame in result.get('frames', [])):
                        errors.append(f'{source.name} {panel["id"]} {environment} {ref}: empty frame')
                checked += 1
    if errors:
        raise SystemExit('\n'.join(errors))
    # Verify backend time macros, environment routing and all bucket options.
    for environment in ['production', 'development']:
        for bucket in ['day', 'week', 'month']:
            query = {'refId': 'A', 'datasource': {'type': 'yesoreyeram-infinity-datasource', 'uid': 'logger-reports-' + environment},
                     'type': 'json', 'source': 'url', 'parser': 'backend', 'format': 'table', 'root_selector': 'series',
                     'url': '/reporting/report?from=${__timeFrom}&to=${__timeTo}&bucket=' + bucket, 'columns': []}
            payload = {'from': str(int(start.timestamp()*1000)), 'to': str(int(now.timestamp()*1000)), 'queries': [query]}
            request = Request(args.url + '/api/ds/query', data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
            with urlopen(request, timeout=30) as response:
                result = json.load(response)['results']['A']
            if result.get('error') or not result.get('frames'):
                raise SystemExit(f'{environment}/{bucket}: {result}')
    print(f'{checked} dashboard panel/environment queries and 6 bucket queries passed.')


if __name__ == '__main__':
    check()
