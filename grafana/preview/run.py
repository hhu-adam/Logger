"""Start isolated Grafana/Prometheus/Logger demo processes, all on localhost."""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--grafana-home', type=Path, required=True)
    parser.add_argument('--plugins', type=Path, required=True)
    parser.add_argument('--prometheus', type=Path, required=True)
    parser.add_argument('--port', type=int, default=13000)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    processes = []
    def stop(*_):
        for p in reversed(processes):
            if p.poll() is None:
                p.terminate()
        for p in processes:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        if _:
            raise SystemExit(0)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with TemporaryDirectory(prefix='logger-grafana-preview-') as temporary:
        root = Path(temporary)
        provisioning = root / 'provisioning'
        dashboards = root / 'dashboards'
        for directory in [dashboards, provisioning / 'dashboards', provisioning / 'datasources']:
            directory.mkdir(parents=True)
        for source in (repo / 'grafana').glob('lean4game-logger-*.json'):
            data = json.loads(source.read_text())
            data['title'] = 'LOCAL DEMO · ' + data['title']
            (dashboards / source.name).write_text(json.dumps(data))
        (provisioning / 'dashboards/logger.yaml').write_text(f'''apiVersion: 1
providers:
  - name: Logger Demo
    type: file
    options:
      path: {dashboards}
''')
        datasource = '''apiVersion: 1
datasources:
  - name: Prometheus Demo
    uid: logger-preview-prometheus
    type: prometheus
    access: proxy
    url: http://127.0.0.1:19090
    isDefault: true
'''
        for name, port in [('production', 18877), ('development', 18878)]:
            datasource += f'''  - name: logger-reports-{name}
    uid: logger-reports-{name}
    type: yesoreyeram-infinity-datasource
    access: proxy
    url: http://127.0.0.1:{port}
    jsonData:
      allowedHosts:
        - http://127.0.0.1:{port}
'''
        (provisioning / 'datasources/logger.yaml').write_text(datasource)
        config = root / 'prometheus.yaml'
        config.write_text('''global:
  scrape_interval: 5s
scrape_configs:
  - job_name: logger
    static_configs:
      - targets: ['127.0.0.1:18877']
        labels:
          environment: production
      - targets: ['127.0.0.1:18878']
        labels:
          environment: development
''')
        for name, port in [('production', 18877), ('development', 18878)]:
            processes.append(subprocess.Popen([sys.executable, '-m', 'Reporting.preview', '--port', str(port), '--environment', name], cwd=repo, stdout=(root / f'{name}.log').open('w'), stderr=subprocess.STDOUT))
        processes.append(subprocess.Popen([str(args.prometheus.resolve()), '--config.file='+str(config), '--storage.tsdb.path='+str(root / 'prometheus-data'), '--web.listen-address=127.0.0.1:19090'], stdout=(root / 'prometheus.log').open('w'), stderr=subprocess.STDOUT))
        env = os.environ.copy()
        env.update(GF_SERVER_HTTP_ADDR='127.0.0.1', GF_SERVER_HTTP_PORT=str(args.port), GF_PATHS_DATA=str(root / 'grafana-data'), GF_PATHS_LOGS=str(root / 'logs'), GF_PATHS_PLUGINS=str(args.plugins.resolve()), GF_PATHS_PROVISIONING=str(provisioning), GF_AUTH_ANONYMOUS_ENABLED='true', GF_AUTH_ANONYMOUS_ORG_ROLE='Admin', GF_AUTH_DISABLE_LOGIN_FORM='true', GF_ANALYTICS_REPORTING_ENABLED='false', GF_ANALYTICS_CHECK_FOR_UPDATES='false', GF_PLUGINS_PREINSTALL_DISABLED='true')
        processes.append(subprocess.Popen([str(args.grafana_home.resolve() / 'bin/grafana'), 'server', '--homepath', str(args.grafana_home.resolve())], env=env, stdout=(root / 'grafana.log').open('w'), stderr=subprocess.STDOUT))
        try:
            for attempt in range(60):
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError('Preview process exited; inspect logs in ' + str(root))
                try:
                    with urlopen(f'http://127.0.0.1:{args.port}/api/health', timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(.5)
            else:
                raise RuntimeError('Grafana startup timed out; logs in ' + str(root))
            print(f'SYNTHETIC LOCAL DEMO: http://127.0.0.1:{args.port}/d/logger-usage\nLogs: {root}', flush=True)
            while all(p.poll() is None for p in processes):
                time.sleep(1)
        except Exception:
            for logfile in root.glob('*.log'):
                print(logfile.name + ':\n' + logfile.read_text()[-3000:], file=sys.stderr)
            raise
        finally:
            stop()


if __name__ == '__main__':
    run()
