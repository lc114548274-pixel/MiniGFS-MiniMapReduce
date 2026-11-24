from __future__ import annotations
import json
import os
import re
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
import httpx
import psutil
from common.utils import atomic_json
from config.settings import DATA, ROOT, MASTER


@contextmanager
def registry_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as stream:
        stream.seek(0)
        if not stream.read(1):
            stream.write(b'0')
            stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == 'nt':
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def check_port(port: int) -> None:
    with socket.socket() as sock:
        try:
            sock.bind(('127.0.0.1', port))
        except OSError as error:
            raise RuntimeError(f'Port {port} is already in use') from error


class Manager:
    def __init__(self, data: Path = DATA):
        self.data = data.resolve()
        self.registry = self.data / 'processes.json'
        self.lock = self.data / 'processes.lock'

    def read(self) -> dict:
        return json.loads(self.registry.read_text(encoding='utf-8')) if self.registry.exists() else {}

    def verified(self, record: dict):
        try:
            process = psutil.Process(record['pid'])
            cmd = process.cmdline()
            if abs(process.create_time()-record['create_time']) > .1 or Path(process.cwd()).resolve() != ROOT.resolve():
                return None
            # The complete launched argument tail must match, not merely the PID or executable name.
            tail = record['args']
            if len(cmd) < len(tail) or cmd[-len(tail):] != tail:
                return None
            return process
        except (psutil.Error, KeyError):
            return None

    def list(self) -> list[dict]:
        with registry_lock(self.lock):
            return [dict(r, name=name, alive=self.verified(r) is not None) for name, r in self.read().items()]

    def start(self, name: str, args: list[str], port: int, master: str = MASTER) -> dict:
        with registry_lock(self.lock):
            records = self.read()
            if name in records and self.verified(records[name]):
                raise RuntimeError(f'{name} is already running')
            check_port(port)
            logs = self.data / 'logs'
            logs.mkdir(parents=True, exist_ok=True)
            environment = {**os.environ, 'MINI_DATA': str(self.data), 'MINI_MASTER': master, 'PYTHONUTF8': '1'}
            options = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {'start_new_session': True}
            with (logs / f'{name}.log').open('ab') as log:
                process = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=environment,
                                           stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, **options)
            record = dict(pid=process.pid, create_time=psutil.Process(process.pid).create_time(),
                          args=args, port=port, master=master)
            records[name] = record
            atomic_json(self.registry, records)
            return record

    def stop(self, name: str) -> dict:
        with registry_lock(self.lock):
            records = self.read()
            if name not in records:
                raise ValueError('Not a project-managed process')
            process = self.verified(records[name])
            if process:
                children = process.children(recursive=True)
                for child in reversed(children):
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        continue
                process.terminate()
                _, alive = psutil.wait_procs([process, *children], timeout=4)
                for item in alive:
                    try:
                        item.kill()
                    except psutil.NoSuchProcess:
                        continue
                psutil.wait_procs(alive, timeout=3)
            return {'name': name, 'status': 'stopped', 'identity_verified': process is not None}

    def worker_action(self, worker_id: str, action: str) -> dict:
        if not re.fullmatch(r'worker-[0-9]+', worker_id):
            raise ValueError('Only project-managed Workers may be controlled')
        records = self.read()
        if worker_id not in records:
            raise ValueError('Worker must first be created by start_cluster')
        if action in ('stop', 'restart'):
            result = self.stop(worker_id)
        if action in ('start', 'restart'):
            record = records[worker_id]
            result = self.start(worker_id, record['args'], record['port'], record['master'])
        return result


def wait_health(url: str, timeout: float = 25) -> dict:
    deadline = time.monotonic()+timeout
    with httpx.Client(timeout=1, trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                response = client.get(url)
                if response.status_code == 200:
                    return response.json() if 'json' in response.headers.get('content-type', '') else {'status': 'ok'}
            except httpx.HTTPError:
                pass
            time.sleep(.25)
    raise RuntimeError('Service failed health check: ' + url)

