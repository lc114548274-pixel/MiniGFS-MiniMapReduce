import os
from pathlib import Path
from common.utils import safe_path, sha, uid


class Storage:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def put(self, chunk_id: str, data: bytes, checksum: str) -> None:
        if sha(data) != checksum:
            raise ValueError('SHA-256 mismatch')
        path = safe_path(self.root, chunk_id)
        if path.exists():
            if sha(path.read_bytes()) != checksum:
                raise ValueError('Immutable chunk already exists with different content')
            return
        temp = safe_path(self.root, uid(), '.tmp')
        try:
            temp.write_bytes(data)
            os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)

    def get(self, chunk_id: str) -> bytes:
        return safe_path(self.root, chunk_id).read_bytes()

