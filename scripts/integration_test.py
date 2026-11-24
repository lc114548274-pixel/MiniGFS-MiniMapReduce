"""Real isolated processes, actual HTTP and Worker termination; no mocks."""
import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
import httpx
from common.utils import sha
from config.settings import ROOT
from minigfs.client import Client
from minimr.algorithms import wordcount, split_samples, monte_carlo
from scripts.process_manager import Manager, wait_health


def await_job(client, job_id, timeout=120):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        job = client.request('GET', '/jobs/'+job_id)
        if job['status'] in ('completed', 'failed'):
            assert job['status'] == 'completed', job.get('error')
            return job
        time.sleep(.1)
    raise TimeoutError('Job timed out')


def ports_available(ports):
    sockets = []
    try:
        for port in ports:
            sock = socket.socket()
            sockets.append(sock)
            sock.bind(('127.0.0.1', port))
        return True
    except OSError:
        return False
    finally:
        for sock in sockets:
            sock.close()


def run(report_path: Path):
    checks = []
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix='minigfs-integration-') as directory:
        root = Path(directory)
        manager = Manager(root)
        base = next(p for p in range(18000, 25000, 10) if ports_available([p, p+1, p+2, p+3]))
        master = f'http://127.0.0.1:{base}'
        environment = {**os.environ, 'MINI_DATA': str(root), 'MINI_MASTER': master, 'MINI_HEARTBEAT': '.5', 'MINI_OFFLINE_AFTER': '2', 'PYTHONUTF8': '1'}
        try:
            subprocess.run([sys.executable, '-m', 'scripts.start_cluster', '--master-port', str(base), '--worker-base', str(base), '--no-dashboard'], cwd=ROOT, env=environment, check=True, timeout=60)
            client = Client(master)
            assert len(client.request('GET', '/workers')) == 3
            checks.append('three_real_workers_registered')
            path = root/'input.txt'
            content = (b'Apple banana APPLE, distributed system!\n'*120000) + ' UTF8中文🙂 end'.encode()
            assert len(content) > 4*1024*1024
            path.write_bytes(content)
            file = client.upload(path)
            assert file['status'] == 'completed' and len(file['chunks']) == 2
            assert all(len(c['replicas']) == 2 and all(r['status'] == 'stored' for r in c['replicas']) for c in file['chunks'])
            checks.append('multi_chunk_double_replication')
            downloaded = root/'download.txt'
            assert client.download(file['file_id'], downloaded)['verified']
            assert downloaded.read_bytes() == content
            checks.append('upload_download_sha256')
            chunk = file['chunks'][0]
            first_replica = root / chunk['replicas'][0]['worker_id'] / 'chunks' / chunk['chunk_id']
            original = first_replica.read_bytes()
            first_replica.write_bytes(b'corrupted-on-disk')
            report = client.download(file['file_id'], downloaded)
            assert report['verified'] and report['fallbacks']
            first_replica.write_bytes(original)
            checks.append('corrupt_replica_checksum_failover')
            for name, data in [('empty.bin', b''), ('binary.bin', bytes(range(256))*40)]:
                source = root/name
                source.write_bytes(data)
                uploaded = client.upload(source, chunk_size=4096)
                target = root/('restored-'+name)
                assert client.download(uploaded['file_id'], target)['verified']
                assert target.read_bytes() == data
            checks.append('empty_and_binary_file_roundtrip')
            boundary = root/'boundary.txt'
            boundary.write_bytes('Alpha中文🙂Beta Supercalifragilistic alpha!'.encode())
            boundary_file = client.upload(boundary, chunk_size=7)
            boundary_job = client.request('POST', '/jobs', json={'job_type': 'wordcount', 'input_file_id': boundary_file['file_id']})
            assert await_job(client, boundary_job['job_id'])['result'] == wordcount(boundary.read_bytes())
            checks.append('real_mapper_utf8_and_long_word_chunk_boundaries')
            # Stop the first candidate then download before heartbeat expiry: exercises actual failed GET + fallback.
            first = file['chunks'][0]['replicas'][0]['worker_id']
            manager.stop(first)
            assert client.download(file['file_id'], downloaded)['verified']
            time.sleep(2.2)
            assert not next(w for w in client.request('GET', '/workers') if w['worker_id'] == first)['online']
            checks.append('real_worker_shutdown_and_replica_failover')
            job = client.request('POST', '/jobs', json={'job_type': 'wordcount', 'input_file_id': file['file_id'], 'worker_count': 3})
            result = await_job(client, job['job_id'])
            assert len(result['map_tasks']) == len(file['chunks'])
            assert result['result'] == wordcount(content)
            checks.append('wordcount_map_shuffle_reduce_matches_serial')
            client.request('POST', f'/processes/{first}/start')
            time.sleep(1.5)
            # Ensure deterministic counts survive a real in-flight Worker kill.
            spec = {'job_type': 'monte_carlo', 'samples': 12000000, 'partitions': 6, 'seed': 42, 'worker_count': 3}
            job = client.request('POST', '/jobs', json=spec)
            deadline = time.monotonic()+10
            victim = None
            while time.monotonic() < deadline:
                current = client.request('GET', '/jobs/'+job['job_id'])
                active = [t for t in current['map_tasks'] if t['status'] == 'running']
                if active:
                    victim = active[0]['assigned_worker']
                    break
                time.sleep(.05)
            assert victim, 'No running task observed for fault injection'
            manager.stop(victim)
            result = await_job(client, job['job_id'])
            assert sum(t['attempt']-1 for t in result['map_tasks']) >= 1
            expected = sum(monte_carlo(**p)['inside_circle'] for p in split_samples(spec['samples'], spec['partitions'], spec['seed']))
            assert result['result']['total_samples'] == spec['samples']
            assert result['result']['inside_circle'] == expected
            checks.append('monte_carlo_actual_inflight_failure_retry_exact_result')
            # A duplicate attempt is idempotent on the same Worker.
            task = next(t for t in result['map_tasks'] if t['status'] == 'completed')
            attempt = task['attempts'][-1]
            worker = next(w for w in client.request('GET', '/workers') if w['worker_id'] == task['assigned_worker'])
            request = dict(task_id=task['task_id'], attempt_id=attempt['attempt_id'], job_id=job['job_id'],
                           task_type='map', job_type='monte_carlo', payload=task['payload'])
            with httpx.Client(timeout=5, trust_env=False) as session:
                duplicate = session.post(worker['url']+'/tasks/map', json=request)
                assert duplicate.status_code == 202 and duplicate.json()['status'] == 'completed'
                altered = session.post(worker['url']+'/tasks/map', json={**request, 'payload': {'samples': 1, 'seed': 9}})
                assert altered.status_code == 409
            checks.append('duplicate_attempt_idempotency_and_conflict_rejection')
            # Metadata survives a Master process restart; Workers register again.
            record = manager.read()['master']
            manager.stop('master')
            manager.start('master', record['args'], record['port'], master)
            wait_health(master+'/health')
            time.sleep(1.5)
            assert client.request('GET', '/files/'+file['file_id'])['status'] == 'completed'
            assert client.download(file['file_id'], downloaded)['verified']
            checks.append('master_restart_persistent_metadata_download')
            # All replicas unavailable: incomplete destination must be cleaned, existing target retained.
            for record in manager.list():
                if record['name'].startswith('worker-'):
                    manager.stop(record['name'])
            preserved = root/'preserved.bin'
            preserved.write_bytes(b'original')
            try:
                client.download(file['file_id'], preserved)
                raise AssertionError('Expected unavailable replicas to fail')
            except IOError:
                assert preserved.read_bytes() == b'original'
                assert not list(root.glob('*.partial'))
            checks.append('all_replicas_unavailable_cleans_partial_preserves_destination')
        finally:
            for record in reversed(manager.list()):
                manager.stop(record['name'])
            assert not any(r['alive'] for r in manager.list())
            checks.append('all_test_processes_stopped')
    report = {'status': 'passed', 'checks': checks, 'seconds': time.perf_counter()-started, 'python': sys.version}
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', type=Path, default=ROOT/'docs'/'integration-results.json')
    run(parser.parse_args().report)
