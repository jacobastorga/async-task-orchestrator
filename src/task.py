import enum
from typing import Callable, Any, Set, Optional

class TaskStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

class Task:
    """Represents an atomic asynchronous work unit within a DAG."""

    def __init__(self, name: str, fn: Callable[..., Any], dependencies: Optional[Set[str]] = None,
                 retries: int = 0, timeout: Optional[float] = None):
        self.name = name
        self.fn = fn
        self.dependencies = set(dependencies) if dependencies else set()
        self.retries = retries
        self.timeout = timeout
        self.status = TaskStatus.PENDING
        self.result: Any = None
        self.error: Optional[Exception] = None
        self.attempts: int = 0

    def __repr__(self) -> str:
        return f"Task(name={self.name!r}, status={self.status.value}, deps={self.dependencies})"
