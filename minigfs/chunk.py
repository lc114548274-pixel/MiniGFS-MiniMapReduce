from collections.abc import Iterator
from pathlib import Path
from config.settings import CHUNK_SIZE


def iter_chunks(path: Path, size: int = CHUNK_SIZE) -> Iterator[bytes]:
    if size <= 0:
        raise ValueError('Chunk size must be positive')
    with path.open('rb') as stream:
        while block := stream.read(size):
            yield block

