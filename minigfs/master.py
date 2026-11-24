import asyncio
import json
from contextlib import asynccontextmanager, suppress
import httpx
from fastapi import FastAPI, HTTPException, Query
from common.models import WorkerInfo, FileCreate, ReplicaUpdate, JobCreate, BenchmarkCreate, FileMetadata, JobMetadata, TaskMetadata
from common.utils import now, uid, setup_logging
from config.settings import DATA, REPLICATION
from minigfs.metadata import Metadata
from minimr.scheduler import Scheduler


def create_app(data_dir=DATA):
    state = Metadata(data_dir / 'master' / 'metadata.json')
    scheduler = Scheduler(state)

    async def monitor():
        while True:
            state.refresh()
            state.history.append({'time': now(), 'workers': [dict(w) for w in state.workers.values()]})
            await asyncio.sleep(3)

    @asynccontextmanager
    async def lifespan(app):
        monitor_task = asyncio.create_task(monitor())
        yield
        monitor_task.cancel()
        for task in scheduler.background:
            task.cancel()
        with suppress(asyncio.CancelledError):
            await monitor_task
        state.save()

    app = FastAPI(title='MiniGFS + MiniMapReduce Master', version='1.0.0', lifespan=lifespan)
    app.state.metadata, app.state.scheduler = state, scheduler

    @app.get('/health')
    async def health():
        return {'status': 'ok', 'service': 'minigfs-master'}

    @app.post('/workers/register')
    @app.post('/workers/heartbeat')
    async def register(info: WorkerInfo):
        state.register(info.model_dump())
        return {'status': 'online'}

    @app.get('/workers')
    async def workers():
        state.refresh()
        return list(state.workers.values())

    @app.post('/files/allocate', status_code=201, response_model=FileMetadata)
    async def allocate(spec: FileCreate):
        online = state.online()
        if not online:
            raise HTTPException(503, 'No online Worker; upload rejected')
        if sum(c.size for c in spec.chunks) != spec.file_size or any(c.size == 0 for c in spec.chunks):
            raise HTTPException(422, 'Chunk sizes must sum to file size; chunks must be nonempty')
        file_id = uid()
        chunks = []
        for index, chunk in enumerate(spec.chunks):
            chosen = [online[(state.rotation+i) % len(online)] for i in range(min(REPLICATION, len(online)))]
            state.rotation += 1
            chunks.append(dict(chunk_id=uid(), index=index, **chunk.model_dump(), status='pending',
                               replicas=[{'worker_id': w['worker_id'], 'status': 'pending'} for w in chosen]))
        file = dict(file_id=file_id, filename=spec.filename, file_size=spec.file_size,
                    checksum=spec.checksum, created_at=now(), status='uploading', chunks=chunks)
        state.files[file_id] = file
        state.event('chunk_allocated', f'{len(chunks)} chunks allocated', file_id=file_id)
        state.save()
        return state.file_view(file)

    def get_file(file_id):
        if file_id not in state.files:
            raise HTTPException(404, 'File not found')
        return state.files[file_id]

    @app.post('/files/{file_id}/replicas', response_model=FileMetadata)
    async def replica(file_id: str, update: ReplicaUpdate):
        file = get_file(file_id)
        for chunk in file['chunks']:
            if chunk['chunk_id'] == update.chunk_id:
                for replica in chunk['replicas']:
                    if replica['worker_id'] == update.worker_id:
                        if replica['status'] != 'stored':
                            if update.status == 'stored':
                                worker = state.workers.get(update.worker_id)
                                if not worker:
                                    raise HTTPException(409, 'Worker registration missing')
                                try:
                                    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
                                        response = await client.get(worker['url'] + '/chunks/' + update.chunk_id)
                                        response.raise_for_status()
                                    from common.utils import sha
                                    if len(response.content) != chunk['size'] or sha(response.content) != chunk['checksum']:
                                        raise ValueError('Checksum mismatch')
                                except (httpx.HTTPError, ValueError) as error:
                                    raise HTTPException(409, 'Replica could not be verified') from error
                            replica['status'] = update.status
                            state.event('replica_' + update.status, update.chunk_id, worker_id=update.worker_id, file_id=file_id)
                        chunk['status'] = 'stored' if any(r['status'] == 'stored' for r in chunk['replicas']) else ('failed' if all(r['status'] == 'failed' for r in chunk['replicas']) else 'pending')
                        state.save()
                        return state.file_view(file)
        raise HTTPException(404, 'Replica not allocated')

    @app.post('/files/{file_id}/complete', response_model=FileMetadata)
    async def complete(file_id: str):
        file = get_file(file_id)
        file['status'] = 'completed' if all(c['status'] == 'stored' for c in file['chunks']) else 'failed'
        state.save()
        state.event('file_' + file['status'], file['filename'], file_id=file_id)
        return state.file_view(file)

    @app.get('/files', response_model=list[FileMetadata])
    async def files():
        return [state.file_view(f) for f in state.files.values()]

    @app.get('/files/{file_id}', response_model=FileMetadata)
    async def file(file_id: str):
        return state.file_view(get_file(file_id))

    @app.get('/files/{file_id}/preview')
    async def preview(file_id: str):
        file = state.file_view(get_file(file_id))
        if not file['chunks']:
            return {'text': '', 'bytes_shown': 0}
        chunk = file['chunks'][0]
        from common.utils import sha
        async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
            for replica in chunk['replicas']:
                if replica['status'] != 'stored' or not replica.get('url'):
                    continue
                try:
                    response = await client.get(replica['url'] + '/chunks/' + chunk['chunk_id'])
                    response.raise_for_status()
                    if sha(response.content) == chunk['checksum']:
                        data = response.content[:2048]
                        return {'text': data.decode('utf-8', errors='replace'), 'bytes_shown': len(data)}
                except httpx.HTTPError:
                    continue
        raise HTTPException(503, 'No valid replica for preview')

    @app.post('/files/{file_id}/download-report')
    async def download_report(file_id: str, report: dict):
        get_file(file_id)
        state.event('download_verified', json.dumps(report), file_id=file_id)
        return {'received': True}

    @app.post('/jobs', status_code=202, response_model=JobMetadata)
    async def create_job(spec: JobCreate):
        try:
            return scheduler.create(spec)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    def get_job(job_id):
        if job_id not in state.jobs:
            raise HTTPException(404, 'Job not found')
        return state.jobs[job_id]

    @app.get('/jobs')
    async def jobs():
        return [{k: v for k, v in j.items() if k not in ('result', 'intermediates', 'shuffle_groups', 'map_tasks', 'reduce_tasks')} for j in state.jobs.values()]

    @app.get('/jobs/{job_id}', response_model=JobMetadata)
    async def job(job_id: str):
        return get_job(job_id)

    @app.get('/jobs/{job_id}/tasks', response_model=list[TaskMetadata])
    async def tasks(job_id: str):
        j = get_job(job_id)
        return j['map_tasks'] + j['reduce_tasks']

    @app.get('/jobs/{job_id}/result')
    async def result(job_id: str):
        j = get_job(job_id)
        if j['status'] != 'completed':
            raise HTTPException(409, 'Job is not completed')
        return j['result']

    @app.get('/events')
    async def events(level: str | None = None, worker_id: str | None = None, job_id: str | None = None, event: str | None = None):
        return [e for e in state.events if all(value is None or e.get(key) == value for key, value in
                [('level', level), ('worker_id', worker_id), ('job_id', job_id), ('event', event)])]

    @app.get('/metrics')
    async def metrics():
        online = state.online()
        chunks = [c for f in state.files.values() for c in f['chunks']]
        return dict(worker_count=len(state.workers), online=len(online), offline=len(state.workers)-len(online),
                    files=len(state.files), chunks=len(chunks), replicas=sum(r['status'] == 'stored' for c in chunks for r in c['replicas']),
                    storage_bytes=sum(w['used'] for w in state.workers.values()),
                    running_tasks=sum(scheduler.busy.values()), completed_jobs=sum(j['status'] == 'completed' for j in state.jobs.values()),
                    history=list(state.history))

    @app.post('/benchmarks', status_code=202)
    async def benchmark(spec: BenchmarkCreate):
        from benchmarks.runner import launch
        if any(b['status'] == 'running' for b in state.benchmarks.values()) or any(j['status'] not in ('completed', 'failed') for j in state.jobs.values()):
            raise HTTPException(409, 'Wait for active jobs/benchmarks to finish')
        if max(spec.worker_counts) > len(state.online()):
            raise HTTPException(422, 'Requested Worker count exceeds online Workers')
        if spec.job_type == 'wordcount' and (not spec.input_file_id or get_file(spec.input_file_id)['status'] != 'completed'):
            raise HTTPException(422, 'Select a completed input file')
        return launch(spec, state, scheduler, data_dir)

    @app.get('/benchmarks')
    async def benchmarks():
        return list(state.benchmarks.values())

    @app.get('/benchmarks/{benchmark_id}')
    async def benchmark_result(benchmark_id: str):
        if benchmark_id not in state.benchmarks:
            raise HTTPException(404, 'Benchmark not found')
        return state.benchmarks[benchmark_id]

    @app.get('/processes')
    async def processes():
        from scripts.process_manager import Manager
        return Manager(data_dir).list()

    @app.post('/processes/{worker_id}/{action}')
    async def manage(worker_id: str, action: str):
        from scripts.process_manager import Manager
        if action not in ('start', 'stop', 'restart'):
            raise HTTPException(422, 'Unsupported action')
        try:
            return await asyncio.to_thread(Manager(data_dir).worker_action, worker_id, action)
        except (ValueError, RuntimeError, OSError) as error:
            raise HTTPException(409, str(error)) from error

    return app


setup_logging('master')
app = create_app()
