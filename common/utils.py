import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def uid() -> str:
    return uuid.uuid4().hex


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_path(root: Path, name: str, suffix: str = '') -> Path:
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', name):
        raise ValueError('Invalid object identifier')
    root.mkdir(parents=True, exist_ok=True)
    path = (root / (name + suffix)).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError('Path outside configured data directory')
    return path


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uid() + '.tmp')
    try:
        with temp.open('w', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def setup_logging(component: str) -> None:
    logging.basicConfig(level=logging.INFO, format=f'%(asctime)s %(levelname)s {component} %(message)s')

