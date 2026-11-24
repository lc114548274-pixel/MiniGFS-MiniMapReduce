import asyncio
import json
import time
from collections import Counter
import httpx
from common.models import JobCreate
from common.utils import now, uid
from config.settings import MAX_RETRIES, TASK_TIMEOUT
from minimr.algorithms import partition, split_samples


class Scheduler:
    def __init__(self, state):
        self.state = state
        self.busy = Counter()
        self.lock = asyncio.Lock()
        self.background = set()

    def create(self, spec: JobCreate) -> dict:
        online = self.state.online()
        if not online:
            raise ValueError('No online Workers')
        if spec.job_type == 'wordcount':
            file = self.state.files.get(spec.input_file_id)
            if not file or file['status'] != 'completed':
                raise ValueError('A completed input file is required')
            inputs = [{'index': c['index'], 'chunk_id': c['chunk_id']} for c in file['chunks']]
        else:
            inputs = split_samples(spec.samples, spec.partitions, spec.seed)
        job_id = uid()
        def task(kind, payload):
            return dict(task_id=uid(), job_id=job_id, task_type=kind, status='pending',
                        chunk_id=payload.get('chunk_id'), payload=payload, assigned_worker=None,
                        attempt=0, attempts=[], started_at=None, finished_at=None,
                        execution_time=None, result_location=None, error_message=None)
        job = dict(job_id=job_id, job_type=spec.job_type, input_file_id=spec.input_file_id,
                   parameters=spec.model_dump(), status='pending', created_at=now(), started_at=None,
                   finished_at=None, execution_time=None, result=None,
                   map_tasks=[task('map', p) for p in inputs], reduce_tasks=[],
                   allowed_workers=[w['worker_id'] for w in online[:spec.worker_count]],
                   phases={}, data_transfer_bytes=0, shuffle_groups={}, intermediates={})
        self.state.jobs[job_id] = job
        self.state.save()
        self.state.event('job_created', spec.job_type, job_id=job_id)
        background = asyncio.create_task(self.run(job, task))
        self.background.add(background)
        background.add_done_callback(self.background.discard)
        return job

    async def acquire(self, job, task, tried):
        deadline = time.monotonic() + 20
        while True:
            async with self.lock:
                workers = [w for w in self.state.online() if w['worker_id'] in job['allowed_workers']]
                eligible = [w for w in workers if self.busy[w['worker_id']] < min(
                    w['slots'], job['parameters']['max_concurrency'])]
                if eligible:
                    local = set()
                    if task['chunk_id']:
                        file = self.state.files[job['input_file_id']]
                        chunk = file['chunks'][task['payload']['index']]
                        local = {r['worker_id'] for r in chunk['replicas'] if r['status'] == 'stored'}
                    chosen = min(eligible, key=lambda w: (w['worker_id'] in tried,
                                 w['worker_id'] not in local, self.busy[w['worker_id']], w['worker_id']))
                    self.busy[chosen['worker_id']] += 1
                    return chosen
                if workers:
                    deadline = time.monotonic() + 20
            if time.monotonic() > deadline:
                raise RuntimeError('No eligible online Worker for 20 seconds')
            await asyncio.sleep(.15)

    async def execute_task(self, job: dict, task: dict) -> dict:
        tried = set()
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            for attempt in range(1, MAX_RETRIES + 2):
                try:
                    worker = await self.acquire(job, task, tried)
                except RuntimeError as error:
                    task.update(status='failed', error_message=str(error), finished_at=now())
                    raise
                worker_id = worker['worker_id']
                attempt_id, tick = uid(), time.perf_counter()
                history = dict(attempt=attempt, attempt_id=attempt_id, worker_id=worker_id,
                               started_at=now(), status='running')
                task['attempts'].append(history)
                task.update(status='running', attempt=attempt, assigned_worker=worker_id,
                            started_at=task['started_at'] or now())
                self.state.event('task_started', task['task_type'], job_id=job['job_id'], task_id=task['task_id'], worker_id=worker_id)
                payload = task['payload']
                if task['chunk_id']:
                    payload = {'index': payload['index'], 'chunks': self.state.file_view(self.state.files[job['input_file_id']])['chunks']}
                request = dict(task_id=task['task_id'], attempt_id=attempt_id, job_id=job['job_id'],
                               task_type=task['task_type'], job_type=job['job_type'], payload=payload)
                try:
                    response = await client.post(worker['url'] + '/tasks/' + task['task_type'], json=request)
                    response.raise_for_status()
                    while True:
                        if time.perf_counter()-tick > TASK_TIMEOUT:
                            raise TimeoutError('Task deadline exceeded')
                        self.state.refresh()
                        current = self.state.workers[worker_id]
                        if not current['online'] or current['instance_id'] != worker['instance_id']:
                            raise RuntimeError('Worker offline or restarted')
                        response = await client.get(worker['url'] + '/tasks/' + attempt_id)
                        response.raise_for_status()
                        status = response.json()
                        if status['status'] == 'failed':
                            raise RuntimeError(status['error_message'])
                        if status['status'] == 'completed':
                            response = await client.get(worker['url'] + status['result_location'])
                            response.raise_for_status()
                            result = response.json()
                            job['data_transfer_bytes'] += len(response.content) + len(json.dumps(request).encode()) + result['data_transfer_bytes']
                            task.update(status='completed', finished_at=now(), execution_time=result['execution_time'],
                                        result_location=f"{worker['url']}{status['result_location']}", error_message=None,
                                        input_bytes=result['input_bytes'])
                            history.update(status='completed', finished_at=now(), execution_time=time.perf_counter()-tick)
                            self.state.event('task_completed', task['task_type'], job_id=job['job_id'], task_id=task['task_id'], worker_id=worker_id)
                            return result['value']
                        await asyncio.sleep(.15)
                except (httpx.HTTPError, RuntimeError, TimeoutError, ValueError) as error:
                    tried.add(worker_id)
                    history.update(status='failed', finished_at=now(), execution_time=time.perf_counter()-tick, error_message=str(error))
                    task.update(status='retrying' if attempt <= MAX_RETRIES else 'failed', error_message=str(error))
                    self.state.event('task_retry' if attempt <= MAX_RETRIES else 'task_failed', str(error), 'WARNING',
                                     job_id=job['job_id'], task_id=task['task_id'], worker_id=worker_id)
                finally:
                    self.busy[worker_id] -= 1
            task['finished_at'] = now()
            raise RuntimeError(f"Task {task['task_id']} exhausted {MAX_RETRIES} retries: {task['error_message']}")

    async def stage(self, job, tasks):
        results = await asyncio.gather(*(self.execute_task(job, t) for t in tasks), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results

    async def run(self, job, make_task):
        tick = time.perf_counter()
        job.update(status='mapping', started_at=now())
        try:
            phase = time.perf_counter()
            values = await self.stage(job, job['map_tasks'])
            job['phases']['map'] = time.perf_counter()-phase
            job['status'] = 'shuffling'
            self.state.event('shuffle_started', 'Master grouping and deterministic SHA-256 partitioning', job_id=job['job_id'])
            phase = time.perf_counter()
            job['intermediates'] = {task['task_id']: value for task, value in zip(job['map_tasks'], values)}
            if job['job_type'] == 'wordcount':
                groups = {}
                for value in values:
                    for key, count in value.items():
                        groups.setdefault(key, []).append(count)
                partitions = [{} for _ in range(job['parameters']['reducers'])]
                for key, counts in groups.items():
                    partitions[partition(key, len(partitions))][key] = counts
                job['shuffle_groups'] = groups
            else:
                partitions = [values]
            job['reduce_tasks'] = [make_task('reduce', {'values': p}) for p in partitions]
            job['phases']['shuffle'] = time.perf_counter()-phase
            self.state.event('shuffle_completed', 'Partitions ready', job_id=job['job_id'])
            job['status'] = 'reducing'
            phase = time.perf_counter()
            reduced = await self.stage(job, job['reduce_tasks'])
            job['phases']['reduce'] = time.perf_counter()-phase
            job['result'] = dict(pair for result in reduced for pair in result.items()) if job['job_type'] == 'wordcount' else reduced[0]
            job['status'] = 'completed'
            self.state.event('job_completed', 'Result committed once per logical task', job_id=job['job_id'])
        except Exception as error:
            job.update(status='failed', error=str(error))
            self.state.event('job_failed', str(error), 'ERROR', job_id=job['job_id'])
        finally:
            job.update(finished_at=now(), execution_time=time.perf_counter()-tick)
            job['phases']['coordination'] = max(0, job['execution_time']-sum(job['phases'].values()))
            self.state.save()

