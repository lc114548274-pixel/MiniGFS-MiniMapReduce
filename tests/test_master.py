import asyncio
import time
import pytest
from fastapi.testclient import TestClient
from common.models import JobCreate
from common.utils import sha
from minigfs.master import create_app
from minigfs.metadata import Metadata
from minimr.scheduler import Scheduler


def info(index):
    return dict(worker_id=f'worker-{index}', port=9000+index, started_at='2026-01-01', capacity=10000,
                used=0, tasks=0, cpu=0, memory=0, slots=1, instance_id=str(index))


def file_spec():
    return dict(filename='a.txt', file_size=6, checksum=sha(b'abcabc'),
                chunks=[dict(size=3, checksum=sha(b'abc'))]*2)


@pytest.mark.parametrize('workers', [1, 2, 3])
def test_replica_allocation_and_persistence(tmp_path, workers):
    app = create_app(tmp_path)
    with TestClient(app) as client:
        for i in range(1, workers+1):
            assert client.post('/workers/register', json=info(i)).status_code == 200
        response = client.post('/files/allocate', json=file_spec())
        assert response.status_code == 201
        file = response.json()
        assert [c['index'] for c in file['chunks']] == [0, 1]
        for chunk in file['chunks']:
            ids = [r['worker_id'] for r in chunk['replicas']]
            assert len(set(ids)) == min(workers, 2)
        persisted = Metadata(tmp_path / 'master' / 'metadata.json')
        assert persisted.files[file['file_id']]['filename'] == 'a.txt'


def test_no_workers_reject_upload(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        assert client.post('/files/allocate', json=file_spec()).status_code == 503


def test_heartbeat_timeout_and_return(tmp_path):
    state = Metadata(tmp_path/'metadata.json')
    state.register(info(1))
    state.workers['worker-1']['_seen'] = time.monotonic()-20
    assert not state.online()
    state.register(info(1))
    assert len(state.online()) == 1
    assert [e['event'] for e in state.events] == ['worker_online', 'worker_offline', 'worker_online']


def test_failed_replica_does_not_erase_success(tmp_path):
    app = create_app(tmp_path)
    with TestClient(app) as client:
        for i in [1, 2]:
            client.post('/workers/register', json=info(i))
        file = client.post('/files/allocate', json=file_spec()).json()
        original = app.state.metadata.files[file['file_id']]
        for chunk in original['chunks']:
            chunk['replicas'][0]['status'] = 'stored'
            second = chunk['replicas'][1]
            response = client.post(f"/files/{file['file_id']}/replicas", json=dict(chunk_id=chunk['chunk_id'], worker_id=second['worker_id'], status='failed'))
            assert response.status_code == 200
        result = client.post(f"/files/{file['file_id']}/complete").json()
        assert result['status'] == 'completed'
        assert all(c['status'] == 'stored' for c in result['chunks'])


def test_reject_invalid_sizes_and_remote_worker(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        assert client.post('/workers/register', json={**info(1), 'host': 'example.com'}).status_code == 422
        client.post('/workers/register', json=info(1))
        assert client.post('/files/allocate', json={**file_spec(), 'file_size': 99}).status_code == 422
        assert client.get('/files/missing').status_code == 404


def test_locality_selection(tmp_path):
    async def scenario():
        state = Metadata(tmp_path/'meta.json')
        state.register(info(1))
        state.register(info(2))
        state.files['f'] = {'chunks': [{'replicas': [{'worker_id': 'worker-2', 'status': 'stored'}]}]}
        scheduler = Scheduler(state)
        worker = await scheduler.acquire({'allowed_workers': ['worker-1', 'worker-2'], 'input_file_id': 'f', 'parameters': {'max_concurrency': 1}},
                                         {'chunk_id': 'c', 'payload': {'index': 0}}, set())
        assert worker['worker_id'] == 'worker-2'
    asyncio.run(scenario())


def test_max_retries(tmp_path):
    # An actual closed localhost port forces real connection failures, not a mocked task result.
    async def scenario():
        import socket
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        state = Metadata(tmp_path/'meta.json')
        state.register({**info(1), 'port': port})
        scheduler = Scheduler(state)
        job = scheduler.create(JobCreate(job_type='monte_carlo', samples=1, partitions=1, worker_count=1))
        await asyncio.gather(*scheduler.background)
        assert job['status'] == 'failed'
        assert job['map_tasks'][0]['attempt'] == 4
        assert len(job['map_tasks'][0]['attempts']) == 4
        assert all(a['status'] == 'failed' for a in job['map_tasks'][0]['attempts'])
    asyncio.run(scenario())

