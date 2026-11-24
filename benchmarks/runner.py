import asyncio
import csv
import multiprocessing
import platform
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import psutil
from common.models import JobCreate
from common.utils import atomic_json, now, uid
from minimr.algorithms import split_samples, monte_carlo, reduce_values, wordcount


def serial(spec: dict, input_path: str | None) -> dict:
    tick = time.perf_counter()
    if spec['job_type'] == 'wordcount':
        result = wordcount(Path(input_path).read_bytes())
    else:
        result = reduce_values('monte_carlo', [monte_carlo(**p) for p in split_samples(spec['samples'], spec['partitions'], spec['seed'])])
    return {'result': result, 'compute_time': time.perf_counter()-tick}


def launch(spec, state, scheduler, data_dir):
    benchmark = dict(benchmark_id=uid(), status='running', created_at=now(), configuration=spec.model_dump(), rows=[], summary=[],
                     environment={'python': sys.version, 'platform': platform.platform(), 'logical_cpus': psutil.cpu_count(),
                                  'physical_cpus': psutil.cpu_count(logical=False), 'memory_bytes': psutil.virtual_memory().total},
                     resource_history=[])
    state.benchmarks[benchmark['benchmark_id']] = benchmark
    background = asyncio.create_task(run(benchmark, spec, state, scheduler, data_dir))
    scheduler.background.add(background)
    background.add_done_callback(scheduler.background.discard)
    state.save()
    return benchmark


async def run(benchmark, spec, state, scheduler, data_dir):
    directory = data_dir / 'benchmarks' / benchmark['benchmark_id']
    directory.mkdir(parents=True, exist_ok=True)
    input_path = None
    pool = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context('spawn'))
    try:
        if spec.job_type == 'wordcount':
            from minigfs.client import Client
            from config.settings import MASTER
            input_path = str(directory / 'input.bin')
            await asyncio.to_thread(Client(MASTER).download, spec.input_file_id, Path(input_path))
        loop = asyncio.get_running_loop()
        references = []
        for repeat in range(spec.repeats):
            tick = time.perf_counter()
            baseline = await loop.run_in_executor(pool, serial, spec.model_dump(), input_path)
            elapsed = time.perf_counter()-tick
            references.append(baseline['compute_time'])
            benchmark['rows'].append(dict(workers=0, repeat=repeat+1, seconds=elapsed,
                                          compute_seconds=baseline['compute_time'], map=baseline['compute_time'], shuffle=0,
                                          reduce=0, coordination=elapsed-baseline['compute_time'], transfer_bytes=0, retries=0))
        for count in spec.worker_counts:
            for repeat in range(spec.repeats):
                if len(state.online()) < count:
                    raise RuntimeError('Insufficient Workers during benchmark')
                job = scheduler.create(JobCreate(**{**spec.model_dump(), 'worker_count': count}))
                while job['status'] not in ('completed', 'failed'):
                    benchmark['resource_history'].append({'time': now(), 'workers': count,
                        'cpu': psutil.cpu_percent(), 'memory_bytes': sum(w['memory'] for w in state.online())})
                    benchmark['resource_history'] = benchmark['resource_history'][-12000:]
                    await asyncio.sleep(.2)
                if job['status'] == 'failed':
                    raise RuntimeError(job.get('error', 'Job failed'))
                result = job['result']
                expected = baseline['result']
                if (result != expected if spec.job_type == 'wordcount' else
                    (result['total_samples'], result['inside_circle']) != (expected['total_samples'], expected['inside_circle'])):
                    raise RuntimeError('Serial/distributed result mismatch')
                tasks = job['map_tasks'] + job['reduce_tasks']
                benchmark['rows'].append(dict(workers=count, repeat=repeat+1, seconds=job['execution_time'],
                    compute_seconds=sum(t['execution_time'] or 0 for t in tasks), **job['phases'],
                    transfer_bytes=job['data_transfer_bytes'], retries=sum(max(0, t['attempt']-1) for t in tasks), job_id=job['job_id']))
                state.save()
        serial_mean = statistics.mean(references)
        for count in spec.worker_counts:
            values = [r['seconds'] for r in benchmark['rows'] if r['workers'] == count]
            avg = statistics.mean(values)
            benchmark['summary'].append(dict(workers=count, samples=spec.samples if spec.job_type == 'monte_carlo' else None,
                mean=avg, median=statistics.median(values), stdev=statistics.stdev(values), minimum=min(values), maximum=max(values),
                serial_compute=serial_mean, speedup=serial_mean/avg, efficiency=serial_mean/avg/count*100))
        benchmark['status'] = 'completed'
    except Exception as error:
        benchmark.update(status='failed', error=str(error))
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        benchmark['finished_at'] = now()
        atomic_json(directory / 'results.json', benchmark)
        if benchmark['rows']:
            fields = list(dict.fromkeys(key for row in benchmark['rows'] for key in row))
            with (directory / 'results.csv').open('w', newline='', encoding='utf-8') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(benchmark['rows'])
        state.save()

