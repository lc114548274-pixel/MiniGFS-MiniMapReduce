import argparse
import importlib
import sys
import time
import httpx
from scripts.process_manager import Manager, check_port, wait_health


def start(workers=3, master_port=8000, worker_base=9000, dashboard_port=8501, dashboard=True, slots=1):
    for module in ('fastapi', 'uvicorn', 'httpx', 'psutil', 'streamlit', 'plotly', 'pandas'):
        importlib.import_module(module)
    master = f'http://127.0.0.1:{master_port}'
    manager, started = Manager(), []
    for port in [master_port, *range(worker_base+1, worker_base+workers+1), *([dashboard_port] if dashboard else [])]:
        check_port(port)
    try:
        manager.start('master', ['-m', 'uvicorn', 'minigfs.master:app', '--host', '127.0.0.1', '--port', str(master_port)], master_port, master)
        started.append('master')
        wait_health(master+'/health')
        for i in range(1, workers+1):
            name, port = f'worker-{i}', worker_base+i
            manager.start(name, ['-m', 'worker.server', '--id', name, '--port', str(port), '--slots', str(slots)], port, master)
            started.append(name)
            wait_health(f'http://127.0.0.1:{port}/health')
        deadline = time.monotonic()+20
        with httpx.Client(timeout=3, trust_env=False) as client:
            while time.monotonic() < deadline:
                if len([w for w in client.get(master+'/workers').json() if w['online']]) == workers:
                    break
                time.sleep(.3)
            else:
                raise RuntimeError('Workers failed to register')
        if dashboard:
            manager.start('dashboard', ['-m', 'streamlit', 'run', 'dashboard/app.py', '--server.address', '127.0.0.1',
                                       '--server.port', str(dashboard_port), '--server.headless', 'true',
                                       '--browser.gatherUsageStats', 'false'], dashboard_port, master)
            started.append('dashboard')
            wait_health(f'http://127.0.0.1:{dashboard_port}/_stcore/health')
        print(f'Master: {master}/docs\nWorkers: {workers}\nDashboard: http://localhost:{dashboard_port}' if dashboard else f'Master: {master}; Workers: {workers}')
    except Exception:
        for name in reversed(started):
            manager.stop(name)
        raise


if __name__ == '__main__':
    if sys.version_info < (3, 11):
        raise SystemExit('Python 3.11+ required')
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=3, choices=range(1, 33))
    parser.add_argument('--master-port', type=int, default=8000)
    parser.add_argument('--worker-base', type=int, default=9000)
    parser.add_argument('--dashboard-port', type=int, default=8501)
    parser.add_argument('--slots', type=int, default=1, choices=range(1, 9))
    parser.add_argument('--no-dashboard', action='store_true')
    args = vars(parser.parse_args())
    args['dashboard'] = not args.pop('no_dashboard')
    start(**args)

