import argparse
import json
import time
from minigfs.client import Client


def main(kind: str):
    parser = argparse.ArgumentParser()
    parser.add_argument('--samples', type=int, default=1000000)
    parser.add_argument('--file-id')
    parser.add_argument('--workers', type=int, nargs='+', default=[1, 2, 3])
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--partitions', type=int, default=6)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--concurrency', type=int, default=1)
    args = parser.parse_args()
    if kind == 'wordcount' and not args.file_id:
        parser.error('--file-id is required')
    client = Client()
    benchmark = client.request('POST', '/benchmarks', json=dict(job_type=kind, input_file_id=args.file_id,
        samples=args.samples, worker_counts=args.workers, repeats=args.repeats, partitions=args.partitions,
        seed=args.seed, max_concurrency=args.concurrency))
    print('Benchmark:', benchmark['benchmark_id'])
    while benchmark['status'] == 'running':
        time.sleep(1)
        benchmark = client.request('GET', '/benchmarks/'+benchmark['benchmark_id'])
    print(json.dumps(benchmark['summary'], indent=2))
    if benchmark['status'] != 'completed':
        raise SystemExit(benchmark.get('error', 'Benchmark failed'))

