"""
Task models for the distributed scheduler.
Modelos de tareas para el planificador distribuido.
"""
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import uuid
import json
import pickle
import base64

from src.shared.logging import get_logger

logger = get_logger(__name__)


class TaskStatus(Enum):
    """Task status enumeration / Enumeración de estado de tarea"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(Enum):
    """Task priority levels / Niveles de prioridad de tarea"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    IDLE = 4


@dataclass
class TaskResult:
    """
    Result of task execution.
    Resultado de ejecución de tarea.
    """
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Optional[str] = None
    execution_time: float = 0.0
    worker_id: Optional[str] = None
    completed_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary / Convertir a diccionario"""
        return {
            'task_id': self.task_id,
            'status': self.status.value,
            'result': self.result,
            'error': self.error,
            'execution_time': self.execution_time,
            'worker_id': self.worker_id,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }


@dataclass
class Task:
    """
    Distributed task definition.
    Definición de tarea distribuida.
    """
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "unnamed_task"
    func: Optional[Callable] = None
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    dependencies: Set[str] = field(default_factory=set)
    timeout: Optional[float] = None
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # State
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    result: Optional[TaskResult] = None
    assigned_worker: Optional[str] = None
    
    # Serialization
    serialized_func: Optional[str] = None
    
    def __post_init__(self):
        """Post-initialization / Post-inicialización"""
        if self.func and not self.serialized_func:
            self.serialize_function()
    
    def serialize_function(self):
        """
        Serialize function for distribution.
        Serializa función para distribución.
        """
        if self.func:
            try:
                # Try dill first if available (better for lambdas and closures)
                try:
                    import dill
                    pickled = dill.dumps(self.func)
                except ImportError:
                    # Fall back to pickle
                    pickled = pickle.dumps(self.func)
                self.serialized_func = base64.b64encode(pickled).decode('utf-8')
            except Exception as e:
                # For testing, just log the error but don't raise
                logger.warning(f"Could not serialize function {self.name}: {str(e)}")
                # Keep func reference for local execution
                self.serialized_func = None
    
    def deserialize_function(self):
        """
        Deserialize function for execution.
        Deserializa función para ejecución.
        """
        if self.serialized_func and not self.func:
            try:
                pickled = base64.b64decode(self.serialized_func.encode('utf-8'))
                # Try dill first if available
                try:
                    import dill
                    self.func = dill.loads(pickled)
                except ImportError:
                    self.func = pickle.loads(pickled)
            except Exception as e:
                logger.error(f"Failed to deserialize function: {str(e)}")
                raise
    
    def is_ready(self, completed_tasks: Set[str]) -> bool:
        """
        Check if task is ready to execute (dependencies met).
        Verifica si la tarea está lista para ejecutar (dependencias cumplidas).
        """
        return self.dependencies.issubset(completed_tasks)
    
    def is_timeout(self) -> bool:
        """
        Check if task has timed out.
        Verifica si la tarea ha expirado.
        """
        if not self.timeout or not self.started_at:
            return False
        
        elapsed = (datetime.utcnow() - self.started_at).total_seconds()
        return elapsed > self.timeout
    
    def should_retry(self) -> bool:
        """
        Check if task should be retried.
        Verifica si la tarea debe reintentarse.
        """
        return (
            self.status in [TaskStatus.FAILED, TaskStatus.TIMEOUT] and
            self.retry_count < self.max_retries
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.
        Convierte a diccionario para serialización.
        """
        return {
            'task_id': self.task_id,
            'name': self.name,
            'serialized_func': self.serialized_func,
            'args': self.args,
            'kwargs': self.kwargs,
            'priority': self.priority.value,
            'dependencies': list(self.dependencies),
            'timeout': self.timeout,
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay,
            'created_at': self.created_at.isoformat(),
            'scheduled_at': self.scheduled_at.isoformat() if self.scheduled_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'status': self.status.value,
            'retry_count': self.retry_count,
            'assigned_worker': self.assigned_worker
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """
        Create task from dictionary.
        Crea tarea desde diccionario.
        """
        task = cls(
            task_id=data.get('task_id', str(uuid.uuid4())),
            name=data.get('name', 'unnamed_task'),
            args=tuple(data.get('args', [])),
            kwargs=data.get('kwargs', {}),
            priority=TaskPriority(data.get('priority', TaskPriority.NORMAL.value)),
            dependencies=set(data.get('dependencies', [])),
            timeout=data.get('timeout'),
            max_retries=data.get('max_retries', 3),
            retry_delay=data.get('retry_delay', 1.0)
        )
        
        # Set serialized function
        task.serialized_func = data.get('serialized_func')
        
        # Set timestamps
        if data.get('created_at'):
            task.created_at = datetime.fromisoformat(data['created_at'])
        if data.get('scheduled_at'):
            task.scheduled_at = datetime.fromisoformat(data['scheduled_at'])
        if data.get('started_at'):
            task.started_at = datetime.fromisoformat(data['started_at'])
        if data.get('completed_at'):
            task.completed_at = datetime.fromisoformat(data['completed_at'])
        
        # Set state
        task.status = TaskStatus(data.get('status', TaskStatus.PENDING.value))
        task.retry_count = data.get('retry_count', 0)
        task.assigned_worker = data.get('assigned_worker')
        
        return task
    
    def to_json(self) -> str:
        """Convert to JSON string / Convertir a cadena JSON"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Task':
        """Create from JSON string / Crear desde cadena JSON"""
        return cls.from_dict(json.loads(json_str))


class TaskGraph:
    """
    Represents a graph of tasks with dependencies.
    Representa un grafo de tareas con dependencias.
    """
    
    def __init__(self):
        """Initialize task graph / Inicializar grafo de tareas"""
        self.tasks: Dict[str, Task] = {}
        self.completed: Set[str] = set()
        self.failed: Set[str] = set()
    
    def add_task(self, task: Task):
        """
        Add task to graph.
        Agrega tarea al grafo.
        """
        self.tasks[task.task_id] = task
    
    def get_ready_tasks(self) -> List[Task]:
        """
        Get tasks that are ready to execute.
        Obtiene tareas listas para ejecutar.
        """
        ready = []
        for task in self.tasks.values():
            if (task.status == TaskStatus.PENDING and 
                task.is_ready(self.completed)):
                ready.append(task)
        
        # Sort by priority
        ready.sort(key=lambda t: (t.priority.value, t.created_at))
        return ready
    
    def mark_completed(self, task_id: str):
        """
        Mark task as completed.
        Marca tarea como completada.
        """
        if task_id in self.tasks:
            self.tasks[task_id].status = TaskStatus.COMPLETED
            self.tasks[task_id].completed_at = datetime.utcnow()
            self.completed.add(task_id)
    
    def mark_failed(self, task_id: str):
        """
        Mark task as failed.
        Marca tarea como fallida.
        """
        if task_id in self.tasks:
            self.tasks[task_id].status = TaskStatus.FAILED
            self.failed.add(task_id)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get graph statistics.
        Obtiene estadísticas del grafo.
        """
        status_counts = {}
        for task in self.tasks.values():
            status = task.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            'total_tasks': len(self.tasks),
            'completed': len(self.completed),
            'failed': len(self.failed),
            'status_counts': status_counts,
            'ready_tasks': len(self.get_ready_tasks())
        }