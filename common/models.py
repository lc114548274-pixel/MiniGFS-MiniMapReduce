from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator


class WorkerInfo(BaseModel):
    worker_id: str = Field(pattern=r'^worker-[0-9]+$')
    port: int = Field(ge=1024, le=65535)
    host: Literal['127.0.0.1'] = '127.0.0.1'
    started_at: str
    capacity: int = Field(ge=0)
    used: int = Field(ge=0)
    tasks: int = Field(ge=0)
    cpu: float = Field(ge=0)
    memory: int = Field(ge=0)
    slots: int = Field(default=1, ge=1, le=8)
    instance_id: str


class ChunkSpec(BaseModel):
    size: int = Field(ge=0, le=16 * 1024 * 1024)
    checksum: str = Field(pattern=r'^[0-9a-f]{64}$')


class FileCreate(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    file_size: int = Field(ge=0)
    checksum: str = Field(pattern=r'^[0-9a-f]{64}$')
    chunks: list[ChunkSpec] = Field(max_length=10000)


class ReplicaUpdate(BaseModel):
    chunk_id: str
    worker_id: str
    status: Literal['stored', 'failed']


class JobCreate(BaseModel):
    job_type: Literal['wordcount', 'monte_carlo']
    input_file_id: str | None = None
    samples: int = Field(default=1000000, ge=1, le=1000000000)
    partitions: int = Field(default=6, ge=1, le=128)
    reducers: int = Field(default=2, ge=1, le=32)
    seed: int = Field(default=42, ge=0, le=2**32-1)
    worker_count: int = Field(default=3, ge=1, le=32)
    max_concurrency: int = Field(default=1, ge=1, le=8)


class TaskRequest(BaseModel):
    task_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    attempt_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    job_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    task_type: Literal['map', 'reduce']
    job_type: Literal['wordcount', 'monte_carlo']
    payload: dict


class BenchmarkCreate(JobCreate):
    repeats: int = Field(default=3, ge=3, le=20)
    worker_counts: list[int] = Field(default_factory=lambda: [1, 2, 3], min_length=1, max_length=16)

    @field_validator('worker_counts')
    @classmethod
    def valid_counts(cls, values: list[int]) -> list[int]:
        if any(v < 1 or v > 32 for v in values):
            raise ValueError('worker counts must be 1..32')
        return sorted(set(values))


class ReplicaMetadata(BaseModel):
    model_config = ConfigDict(extra='allow')
    worker_id: str
    status: Literal['pending', 'stored', 'failed']
    online: bool = False
    url: str | None = None


class ChunkMetadata(BaseModel):
    model_config = ConfigDict(extra='allow')
    chunk_id: str
    index: int
    size: int
    checksum: str
    replicas: list[ReplicaMetadata]
    status: Literal['pending', 'stored', 'failed']


class FileMetadata(BaseModel):
    model_config = ConfigDict(extra='allow')
    file_id: str
    filename: str
    file_size: int
    checksum: str
    created_at: str
    chunks: list[ChunkMetadata]
    status: Literal['uploading', 'completed', 'failed']


class TaskMetadata(BaseModel):
    model_config = ConfigDict(extra='allow')
    task_id: str
    job_id: str
    task_type: Literal['map', 'reduce']
    status: Literal['pending', 'running', 'completed', 'failed', 'retrying']
    attempt: int
    attempts: list[dict]
    assigned_worker: str | None
    started_at: str | None
    finished_at: str | None
    execution_time: float | None
    result_location: str | None
    error_message: str | None


class JobMetadata(BaseModel):
    model_config = ConfigDict(extra='allow')
    job_id: str
    job_type: Literal['wordcount', 'monte_carlo']
    input_file_id: str | None
    parameters: dict
    status: Literal['pending', 'mapping', 'shuffling', 'reducing', 'completed', 'failed']
    map_tasks: list[TaskMetadata]
    reduce_tasks: list[TaskMetadata]
    created_at: str
    started_at: str | None
    finished_at: str | None
    execution_time: float | None
    result: dict | None

