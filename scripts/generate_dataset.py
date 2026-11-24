import argparse
import random
from pathlib import Path


def generate(path: Path, size: int, seed: int = 42):
    rng = random.Random(seed)
    words = ['MapReduce', 'distributed', 'storage', 'apple', 'banana', 'worker', 'Python', 'data', 'reliable', 'parallel']
    path.parent.mkdir(parents=True, exist_ok=True)
    remaining = size
    with path.open('wb') as output:
        while remaining:
            block = (' '.join(rng.choices(words, k=2048)) + '\n').encode()
            output.write(block[:remaining])
            remaining -= min(remaining, len(block))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mb', type=float, default=1)
    parser.add_argument('--output', type=Path, default=Path('data/datasets/sample.txt'))
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    if args.mb <= 0:
        parser.error('--mb must be positive')
    generate(args.output, int(args.mb*1024*1024), args.seed)
    print(args.output.resolve())

