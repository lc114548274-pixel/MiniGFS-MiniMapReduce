import time
from pathlib import Path
import httpx
from common.utils import sha, safe_path
from minimr.algorithms import is_letter, owned_text, wordcount, monte_carlo, reduce_values


def execute(request: dict, storage_root: str) -> dict:
    """Runs in a spawned CPU process, never in the HTTP event loop."""
    started, transferred = time.perf_counter(), 0
    payload = request['payload']
    if request['task_type'] == 'reduce':
        result = reduce_values(request['job_type'], payload['values'])
    elif request['job_type'] == 'monte_carlo':
        result = monte_carlo(**payload)
    else:
        def fetch(chunk: dict) -> bytes:
            nonlocal transferred
            local = safe_path(Path(storage_root), chunk['chunk_id'])
            if local.exists():
                data = local.read_bytes()
                if sha(data) == chunk['checksum'] and len(data) == chunk['size']:
                    return data
            with httpx.Client(timeout=10, trust_env=False) as client:
                for replica in chunk['replicas']:
                    if replica['status'] != 'stored' or not replica.get('url'):
                        continue
                    try:
                        response = client.get(replica['url'] + '/chunks/' + chunk['chunk_id'])
                        response.raise_for_status()
                        data = response.content
                        transferred += len(data)
                        if sha(data) == chunk['checksum'] and len(data) == chunk['size']:
                            return data
                    except httpx.HTTPError:
                        continue
            raise IOError('No valid replica: ' + chunk['chunk_id'])

        chunks, index = payload['chunks'], payload['index']
        current = fetch(chunks[index])
        previous = fetch(chunks[index-1])[-1:] if index else b''
        following = []
        if current and is_letter(current[-1]) and not (previous and all(is_letter(c) for c in current) and is_letter(previous[-1])):
            for chunk in chunks[index+1:]:
                block = fetch(chunk)
                following.append(block)
                if any(not is_letter(c) for c in block):
                    break
        text = owned_text(current, previous, following)
        result = wordcount(text)
    return {'value': result, 'execution_time': time.perf_counter()-started,
            'input_bytes': payload.get('chunks', [{}])[payload.get('index', 0)].get('size', 0),
            'data_transfer_bytes': transferred}

