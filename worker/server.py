import argparse
import asyncio
import json
import logging
import multiprocessing
import shutil
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager, suppress
from pathlib import Path
import httpx
import psutil
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request, Response
from common.models import TaskRequest
from common.utils import atomic_json, now, safe_path, setup_logging, uid
from config.settings import DATA, MASTER, HEARTBEAT, SLOTS
from worker.storage import Storage
from worker.executor import execute


def create_app(worker_id: str, port: int, root: Path, master: str = MASTER, slots: int = SLOTS) -> FastAPI:
    storage = Storage(root / 'chunks')
    tasks, running = {}, set()
    instance, started = uid(), now()
    process = psutil.Process()

    async def heartbeat():
        async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
            while True:
                memory = process.memory_info().rss
                for child in process.children(recursive=True):
                    with suppress(psutil.Error):
                        memory += child.memory_info().rss
                info = dict(worker_id=worker_id, port=port, started_at=started,
                            capacity=shutil.disk_usage(root).total,
                            used=sum(p.stat().st_size for p in root.rglob('*') if p.is_file()),
                            tasks=sum(v['status'] == 'running' for v in tasks.values()),
                            cpu=psutil.cpu_percent(), memory=memory, slots=slots, instance_id=instance)
                try:
                    response = await client.post(master + '/workers/heartbeat', json=info)
                    response.raise_for_status()
                except httpx.HTTPError:
                    pass  # Retry a temporarily unavailable Master on the next heartbeat.
                await asyncio.sleep(HEARTBEAT)

    @asynccontextmanager
    async def lifespan(app):
        app.state.pool = ProcessPoolExecutor(max_workers=slots, mp_context=multiprocessing.get_context('spawn'))
        pulse = asyncio.create_task(heartbeat())
        yield
        pulse.cancel()
        with suppress(asyncio.CancelledError):
            await pulse
        for task in running:
            task.cancel()
        app.state.pool.shutdown(wait=False, cancel_futures=True)

    app = FastAPI(title=f'MiniGFS {worker_id}', lifespan=lifespan)

    @app.get('/health')
    async def health():
        return {'status': 'ok', 'worker_id': worker_id, 'instance_id': instance}

    @app.put('/chunks/{chunk_id}')
    async def put_chunk(chunk_id: str, request: Request, x_checksum: str = Header()):
        data = await request.body()
        if len(data) > 16 * 1024 * 1024:
            raise HTTPException(413, 'Chunk exceeds 16 MiB')
        try:
            storage.put(chunk_id, data, x_checksum)
        except ValueError as error:
            raise HTTPException(409, str(error)) from error
        logging.info('worker_id=%s job_id=- task_id=- event=chunk_stored chunk_id=%s bytes=%s', worker_id, chunk_id, len(data))
        return {'status': 'stored', 'size': len(data), 'checksum': x_checksum}

    @app.get('/chunks/{chunk_id}')
    async def get_chunk(chunk_id: str):
        try:
            return Response(storage.get(chunk_id), media_type='application/octet-stream')
        except (FileNotFoundError, ValueError) as error:
            raise HTTPException(404, str(error)) from error

    async def compute(request: TaskRequest):
        record = tasks[request.attempt_id]
        logging.info('worker_id=%s job_id=%s task_id=%s event=task_started attempt_id=%s', worker_id, request.job_id, request.task_id, request.attempt_id)
        try:
            result = await asyncio.get_running_loop().run_in_executor(
                app.state.pool, execute, request.model_dump(), str(storage.root))
            atomic_json(safe_path(root / 'results', request.attempt_id, '.json'), result)
            record.update(status='completed', finished_at=now(), result_location=f'/results/{request.attempt_id}')
            logging.info('worker_id=%s job_id=%s task_id=%s event=task_completed attempt_id=%s', worker_id, request.job_id, request.task_id, request.attempt_id)
        except Exception as error:
            record.update(status='failed', finished_at=now(), error_message=str(error))
            logging.error('worker_id=%s job_id=%s task_id=%s event=task_failed error=%s', worker_id, request.job_id, request.task_id, error)

    @app.post('/tasks/map', status_code=202)
    @app.post('/tasks/reduce', status_code=202)
    async def submit(request: TaskRequest):
        if request.attempt_id in tasks:
            if tasks[request.attempt_id]['request'] != request.model_dump():
                raise HTTPException(409, 'Attempt ID reused with different payload')
            return {k: v for k, v in tasks[request.attempt_id].items() if k != 'request'}
        if sum(t['status'] == 'running' for t in tasks.values()) >= slots:
            raise HTTPException(429, 'Worker slots are full')
        record = {'status': 'running', 'attempt_id': request.attempt_id, 'task_id': request.task_id,
                  'started_at': now(), 'request': request.model_dump()}
        tasks[request.attempt_id] = record
        background = asyncio.create_task(compute(request))
        running.add(background)
        background.add_done_callback(running.discard)
        if len(tasks) > 2000:
            for key in list(tasks):
                if tasks[key]['status'] != 'running' and len(tasks) > 1500:
                    del tasks[key]
        return {k: v for k, v in record.items() if k != 'request'}

    @app.get('/tasks/{attempt_id}')
    async def task_status(attempt_id: str):
        if attempt_id not in tasks:
            raise HTTPException(404, 'Unknown attempt (possibly Worker restarted)')
        return {k: v for k, v in tasks[attempt_id].items() if k != 'request'}

    @app.get('/results/{attempt_id}')
    async def result(attempt_id: str):
        try:
            return json.loads(safe_path(root / 'results', attempt_id, '.json').read_text(encoding='utf-8'))
        except (FileNotFoundError, ValueError) as error:
            raise HTTPException(404, str(error)) from error

    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--slots', type=int, default=SLOTS)
    args = parser.parse_args()
    setup_logging(args.id)
    uvicorn.run(create_app(args.id, args.port, DATA / args.id, slots=args.slots), host='127.0.0.1', port=args.port)
