import copy
import json
import logging
import time
from collections import deque
from pathlib import Path
from common.utils import atomic_json, now
from config.settings import OFFLINE_AFTER


class Metadata:
    """Mutated only on the Master's single asyncio event-loop thread."""

    def __init__(self, path: Path):
        self.path = path
        saved = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        self.files = saved.get('files', {})
        self.jobs = saved.get('jobs', {})
        self.benchmarks = saved.get('benchmarks', {})
        self.workers: dict = {}
        self.events: deque = deque(maxlen=2000)
        self.history: deque = deque(maxlen=1200)
        self.rotation = 0
        for job in self.jobs.values():
            if job['status'] not in ('completed', 'failed'):
                job.update(status='failed', error='Master restarted; submit a new job', finished_at=now())
        for bench in self.benchmarks.values():
            if bench['status'] == 'running':
                bench.update(status='failed', error='Master restarted')

    def save(self) -> None:
        atomic_json(self.path, {'files': self.files, 'jobs': self.jobs, 'benchmarks': self.benchmarks})

    def event(self, event: str, message: str, level: str = 'INFO', **fields) -> None:
        entry = dict(time=now(), level=level, component='master', worker_id=None,
                     job_id=None, task_id=None, event=event, message=message)
        entry.update(fields)
        self.events.append(entry)
        logging.log(getattr(logging, level), '%s', entry)

    def register(self, info: dict) -> None:
        previous = self.workers.get(info['worker_id'])
        online = previous and previous['online']
        self.workers[info['worker_id']] = {**info, 'url': f"http://127.0.0.1:{info['port']}",
                                         'last_heartbeat': now(), '_seen': time.monotonic(), 'online': True}
        if not online:
            self.event('worker_online', 'Worker registered/returned', worker_id=info['worker_id'])

    def refresh(self) -> None:
        for worker in self.workers.values():
            if worker['online'] and time.monotonic() - worker['_seen'] > OFFLINE_AFTER:
                worker['online'] = False
                self.event('worker_offline', 'Heartbeat expired', 'WARNING', worker_id=worker['worker_id'])

    def online(self) -> list[dict]:
        self.refresh()
        return [w for w in self.workers.values() if w['online']]

    def file_view(self, file: dict) -> dict:
        self.refresh()
        result = copy.deepcopy(file)
        for chunk in result['chunks']:
            available = 0
            for replica in chunk['replicas']:
                worker = self.workers.get(replica['worker_id'], {})
                replica['online'] = worker.get('online', False)
                replica['url'] = worker.get('url')
                available += replica['status'] == 'stored' and replica['online']
            chunk['health'] = 'Healthy' if available >= 2 else 'Degraded' if available else 'Unavailable'
        result['warnings'] = [f"Chunk {c['index']}: {c['health']}" for c in result['chunks'] if c['health'] != 'Healthy']
        return result

