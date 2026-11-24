from collections import Counter
import pytest
from common.utils import sha, safe_path
from minigfs.chunk import iter_chunks
from minimr.algorithms import wordcount, owned_text, partition, split_samples, monte_carlo, reduce_values
from worker.storage import Storage


@pytest.mark.parametrize('size', [1, 2, 3, 4, 7, 16, 31, 100])
def test_chunk_boundary_equivalence(tmp_path, size):
    content = ("APPLE banana, Foo-bar can't 中文🙂café\nend " + 'x'*150 + ' Tail').encode()
    path = tmp_path / 'input'
    path.write_bytes(content)
    chunks = list(iter_chunks(path, size))
    assert b''.join(chunks) == content
    total = Counter()
    for index, data in enumerate(chunks):
        owned = owned_text(data, chunks[index-1][-1:] if index else b'', chunks[index+1:])
        total.update(wordcount(owned))
    assert total == wordcount(content)


def test_english_word_rules():
    assert wordcount(b'Apple APPLE, banana! one-two') == {'apple': 2, 'banana': 1, 'one': 1, 'two': 1}


@pytest.mark.parametrize('samples,parts', [(1, 6), (7, 3), (10001, 7)])
def test_sampling_reproducible(samples, parts):
    specs = split_samples(samples, parts, 42)
    assert sum(p['samples'] for p in specs) == samples
    assert len({p['seed'] for p in specs}) == len(specs)
    a = [monte_carlo(**p) for p in specs]
    b = [monte_carlo(**p) for p in specs]
    assert [r['inside_circle'] for r in a] == [r['inside_circle'] for r in b]
    result = reduce_values('monte_carlo', a)
    assert result['total_samples'] == samples
    assert sum(batch['samples'] for batch in result['batches']) == samples
    assert sum(batch['inside_circle'] for batch in result['batches']) == result['inside_circle']
    assert len(result['points']) <= 2000


def test_shuffle_reduce():
    assert partition('apple', 3) == partition('apple', 3)
    assert 0 <= partition('banana', 3) < 3
    assert reduce_values('wordcount', {'apple': [1, 2], 'banana': [3]}) == {'apple': 3, 'banana': 3}


def test_storage_immutable_and_checksum(tmp_path):
    storage = Storage(tmp_path)
    storage.put('chunk1', b'hello', sha(b'hello'))
    storage.put('chunk1', b'hello', sha(b'hello'))
    assert storage.get('chunk1') == b'hello'
    with pytest.raises(ValueError):
        storage.put('chunk2', b'bad', sha(b'good'))
    with pytest.raises(ValueError):
        storage.put('chunk1', b'other', sha(b'other'))


@pytest.mark.parametrize('name', ['../escape', '../../etc', 'a/b', 'a\\b', '', 'x:secret', '/absolute'])
def test_path_safety(tmp_path, name):
    with pytest.raises(ValueError):
        safe_path(tmp_path, name)

