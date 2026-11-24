import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.getenv('MINI_DATA', str(ROOT / 'data'))).resolve()
MASTER = os.getenv('MINI_MASTER', 'http://127.0.0.1:8000')
CHUNK_SIZE = 4 * 1024 * 1024
REPLICATION = 2
HEARTBEAT = float(os.getenv('MINI_HEARTBEAT', '3'))
OFFLINE_AFTER = float(os.getenv('MINI_OFFLINE_AFTER', '10'))
SLOTS = int(os.getenv('MINI_SLOTS', '1'))
MAX_RETRIES = 3
TASK_TIMEOUT = float(os.getenv('MINI_TASK_TIMEOUT', '600'))

