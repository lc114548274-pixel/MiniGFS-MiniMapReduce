import hashlib
import random
import re
import time
from collections import Counter

WORD = re.compile(rb'[A-Za-z]+')


def wordcount(data: bytes) -> dict[str, int]:
    return dict(Counter(word.decode('ascii').lower() for word in WORD.findall(data)))


def is_letter(value: int) -> bool:
    return 65 <= value <= 90 or 97 <= value <= 122


def owned_text(data: bytes, previous: bytes, following: list[bytes]) -> bytes:
    """A token belongs to the chunk containing its first ASCII letter."""
    start = 0
    if previous and is_letter(previous[-1]):
        while start < len(data) and is_letter(data[start]):
            start += 1
    owned = data[start:]
    if not owned or not is_letter(owned[-1]):
        return owned
    suffix = bytearray()
    for block in following:
        for char in block:
            if not is_letter(char):
                return owned + bytes(suffix)
            suffix.append(char)
    return owned + bytes(suffix)


def partition(key: str, reducers: int) -> int:
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % reducers


def split_samples(samples: int, partitions: int, seed: int) -> list[dict]:
    partitions = min(samples, partitions)
    quotient, remainder = divmod(samples, partitions)
    return [{'samples': quotient + (i < remainder),
             'seed': int.from_bytes(hashlib.sha256(f'{seed}:{i}'.encode()).digest()[:8], 'big')}
            for i in range(partitions)]


def monte_carlo(samples: int, seed: int) -> dict:
    started = time.perf_counter()
    rng, inside, points, batches = random.Random(seed), 0, [], []
    interval = max(1, samples // 100)
    last_count = last_inside = 0
    for i in range(1, samples + 1):
        x, y = rng.random(), rng.random()
        hit = x*x + y*y <= 1
        inside += hit
        if len(points) < 150:
            points.append([x, y, hit])
        if i % interval == 0 or i == samples:
            batches.append({'samples': i-last_count, 'inside_circle': inside-last_inside})
            last_count, last_inside = i, inside
    return {'total_samples': samples, 'inside_circle': inside, 'points': points,
            'batches': batches, 'compute_time': time.perf_counter()-started}


def reduce_values(job_type: str, values) -> dict:
    if job_type == 'wordcount':
        return {key: sum(counts) for key, counts in values.items()}
    total = sum(v['total_samples'] for v in values)
    inside = sum(v['inside_circle'] for v in values)
    points = [p for v in values for p in v['points']][:2000]
    batches = [b for v in values for b in v['batches']]
    return {'total_samples': total, 'inside_circle': inside, 'pi': 4*inside/total,
            'points': points, 'batches': batches}

