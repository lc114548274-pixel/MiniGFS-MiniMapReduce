import argparse
import hashlib
import os
from pathlib import Path
import httpx
from common.utils import sha, uid
from config.settings import MASTER, CHUNK_SIZE
from minigfs.chunk import iter_chunks


class Client:
    def __init__(self, master: str = MASTER):
        self.master = master.rstrip('/')

    def request(self, method: str, path: str, **kwargs):
        with httpx.Client(timeout=30, trust_env=False) as session:
            response = session.request(method, self.master + path, **kwargs)
            response.raise_for_status()
            return response.json()

    def upload(self, path: Path, chunk_size: int = CHUNK_SIZE, progress=None) -> dict:
        specs, digest = [], hashlib.sha256()
        for data in iter_chunks(path, chunk_size):
            specs.append({'size': len(data), 'checksum': sha(data)})
            digest.update(data)
        metadata = self.request('POST', '/files/allocate', json={
            'filename': path.name, 'file_size': path.stat().st_size,
            'checksum': digest.hexdigest(), 'chunks': specs})
        with httpx.Client(timeout=30, trust_env=False) as session:
            for index, data in enumerate(iter_chunks(path, chunk_size)):
                chunk = metadata['chunks'][index]
                for replica in chunk['replicas']:
                    status = 'failed'
                    try:
                        response = session.put(replica['url'] + '/chunks/' + chunk['chunk_id'],
                                               content=data, headers={'X-Checksum': chunk['checksum']})
                        response.raise_for_status()
                        status = 'stored'
                    except httpx.HTTPError:
                        status = 'failed'
                    self.request('POST', f"/files/{metadata['file_id']}/replicas", json={
                        'chunk_id': chunk['chunk_id'], 'worker_id': replica['worker_id'], 'status': status})
                if progress:
                    progress((index + 1) / len(specs))
        return self.request('POST', f"/files/{metadata['file_id']}/complete")

    def download(self, file_id: str, target: Path) -> dict:
        metadata = self.request('GET', f'/files/{file_id}')
        if metadata['status'] != 'completed':
            raise ValueError('File is not complete')
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + '.' + uid() + '.partial')
        digest, total, fallbacks = hashlib.sha256(), 0, []
        try:
            with temporary.open('wb') as output, httpx.Client(timeout=10, trust_env=False) as session:
                for chunk in sorted(metadata['chunks'], key=lambda c: c['index']):
                    candidates = sorted(chunk['replicas'], key=lambda r: not r['online'])
                    for replica in candidates:
                        if replica['status'] != 'stored' or not replica.get('url'):
                            continue
                        try:
                            response = session.get(replica['url'] + '/chunks/' + chunk['chunk_id'])
                            response.raise_for_status()
                            data = response.content
                            if len(data) != chunk['size'] or sha(data) != chunk['checksum']:
                                raise ValueError('Replica checksum mismatch')
                            output.write(data)
                            digest.update(data)
                            total += len(data)
                            break
                        except (httpx.HTTPError, ValueError) as error:
                            fallbacks.append({'chunk_id': chunk['chunk_id'], 'worker_id': replica['worker_id'], 'error': str(error)})
                    else:
                        raise IOError(f"All replicas unavailable for chunk {chunk['index']}")
            if total != metadata['file_size'] or digest.hexdigest() != metadata['checksum']:
                raise ValueError('File size/SHA-256 mismatch')
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        report = {'sha256': digest.hexdigest(), 'bytes': total, 'verified': True, 'fallbacks': fallbacks}
        self.request('POST', f'/files/{file_id}/download-report', json=report)
        return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['upload', 'download'])
    parser.add_argument('path', type=Path)
    parser.add_argument('--file-id')
    parser.add_argument('--master', default=MASTER)
    args = parser.parse_args()
    client = Client(args.master)
    print(client.upload(args.path) if args.action == 'upload' else client.download(args.file_id, args.path))

