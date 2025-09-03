"""
Worker implementation for distributed task execution.
Implementación de worker para ejecución distribuida de tareas.
"""
import os
import time
import threading
import multiprocessing as mp
from multiprocessing import Process, Queue, Event
from typing import Optional, Dict, Any, List
from datetime import datetime
from dataclasses import dataclass, field
import uuid

from src.scheduler.task import Task, TaskResult, TaskStatus
from src.shared.logging import get_logger

logger = get_logger(__name__)


@dataclass
class WorkerStats:
    """
    Worker statistics.
    Estadísticas del worker.
    """
    worker_id: str
    tasks_executed: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    total_execution_time: float = 0.0
    current_task: Optional[str] = None
    status: str = "idle"
    started_at: datetime = field(default_factory=datetime.utcnow)
    last_task_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary / Convertir a diccionario"""
        runtime = (datetime.utcnow() - self.started_at).total_seconds()
        return {
            'worker_id': self.worker_id,
            'tasks_executed': self.tasks_executed,
            'tasks_succeeded': self.tasks_succeeded,
            'tasks_failed': self.tasks_failed,
            'total_execution_time': self.total_execution_time,
            'average_execution_time': (
                self.total_execution_time / self.tasks_executed 
                if self.tasks_executed > 0 else 0
            ),
            'current_task': self.current_task,
            'status': self.status,
            'runtime_seconds': runtime,
            'tasks_per_second': self.tasks_executed / runtime if runtime > 0 else 0,
            'last_task_at': self.last_task_at.isoformat() if self.last_task_at else None
        }


class Worker:
    """
    Worker process for task execution.
    Proceso worker para ejecución de tareas.
    """
    
    def __init__(self, worker_id: Optional[str] = None):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique worker identifier
        """
        self.worker_id = worker_id or str(uuid.uuid4())
        self.stats = WorkerStats(worker_id=self.worker_id)
        self.process: Optional[Process] = None
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.shutdown_event = Event()
        self.is_available = True
        self._lock = threading.Lock()
        
        logger.info(f"Worker {self.worker_id} initialized")
    
    def start(self):
        """
        Start worker process.
        Inicia proceso worker.
        """
        if self.process and self.process.is_alive():
            logger.warning(f"Worker {self.worker_id} already running")
            return
        
        self.process = Process(
            target=self._worker_loop,
            args=(self.task_queue, self.result_queue, self.shutdown_event),
            name=f"Worker-{self.worker_id}"
        )
        self.process.daemon = True
        self.process.start()
        
        logger.info(f"Worker {self.worker_id} started")
    
    def _worker_loop(self, task_queue: Queue, result_queue: Queue, shutdown_event: Event):
        """
        Main worker loop running in separate process.
        Bucle principal del worker ejecutándose en proceso separado.
        """
        # Set process name
        try:
            import setproctitle
            setproctitle.setproctitle(f"StreamScale-Worker-{self.worker_id}")
        except ImportError:
            pass
        
        logger.info(f"Worker {self.worker_id} loop started in process {os.getpid()}")
        
        while not shutdown_event.is_set():
            try:
                # Get task from queue with timeout
                task = task_queue.get(timeout=1)
                
                if task is None:
                    # Poison pill received
                    break
                
                # Execute task
                result = self._execute_task(task)
                
                # Put result in queue
                result_queue.put(result)
                
            except Exception as e:
                if not shutdown_event.is_set():
                    logger.error(f"Worker {self.worker_id} error: {str(e)}")
        
        logger.info(f"Worker {self.worker_id} loop stopped")
    
    def _execute_task(self, task: Task) -> TaskResult:
        """
        Execute a single task.
        Ejecuta una única tarea.
        """
        logger.info(f"Worker {self.worker_id} executing task {task.task_id}")
        
        start_time = time.time()
        
        # Update stats
        self.stats.status = "busy"
        self.stats.current_task = task.task_id
        
        try:
            # Deserialize function if needed
            if not task.func:
                task.deserialize_function()
            
            # Execute function
            result = task.func(*task.args, **task.kwargs)
            
            execution_time = time.time() - start_time
            
            # Update stats
            self.stats.tasks_executed += 1
            self.stats.tasks_succeeded += 1
            self.stats.total_execution_time += execution_time
            self.stats.last_task_at = datetime.utcnow()
            
            # Create result
            task_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                result=result,
                execution_time=execution_time,
                worker_id=self.worker_id,
                completed_at=datetime.utcnow()
            )
            
            logger.info(f"Worker {self.worker_id} completed task {task.task_id}")
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # Update stats
            self.stats.tasks_executed += 1
            self.stats.tasks_failed += 1
            self.stats.total_execution_time += execution_time
            
            # Create error result
            task_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=str(e),
                execution_time=execution_time,
                worker_id=self.worker_id
            )
            
            logger.error(f"Worker {self.worker_id} failed task {task.task_id}: {str(e)}")
        
        finally:
            # Reset status
            self.stats.status = "idle"
            self.stats.current_task = None
        
        return task_result
    
    def submit_task(self, task: Task) -> bool:
        """
        Submit task to worker.
        Envía tarea al worker.
        """
        with self._lock:
            if not self.is_available:
                return False
            
            self.is_available = False
            self.task_queue.put(task)
            return True
    
    def get_result(self, timeout: Optional[float] = None) -> Optional[TaskResult]:
        """
        Get result from worker.
        Obtiene resultado del worker.
        """
        try:
            result = self.result_queue.get(timeout=timeout)
            with self._lock:
                self.is_available = True
            return result
        except:
            return None
    
    def is_alive(self) -> bool:
        """
        Check if worker process is alive.
        Verifica si el proceso worker está vivo.
        """
        return self.process and self.process.is_alive()
    
    def shutdown(self):
        """
        Shutdown worker gracefully.
        Apaga el worker ordenadamente.
        """
        logger.info(f"Shutting down worker {self.worker_id}")
        
        # Set shutdown event
        self.shutdown_event.set()
        
        # Send poison pill
        self.task_queue.put(None)
        
        # Wait for process to finish
        if self.process:
            self.process.join(timeout=5)
            
            # Force terminate if still alive
            if self.process.is_alive():
                logger.warning(f"Force terminating worker {self.worker_id}")
                self.process.terminate()
                self.process.join()
        
        logger.info(f"Worker {self.worker_id} shutdown complete")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get worker statistics.
        Obtiene estadísticas del worker.
        """
        return self.stats.to_dict()


class WorkerPool:
    """
    Pool of worker processes.
    Pool de procesos worker.
    """
    
    def __init__(self, num_workers: int = 4):
        """
        Initialize worker pool.
        
        Args:
            num_workers: Number of workers in pool
        """
        self.num_workers = num_workers
        self.workers: List[Worker] = []
        self.available_workers: List[Worker] = []
        self._lock = threading.Lock()
        
        # Create workers
        for i in range(num_workers):
            worker = Worker(worker_id=f"worker_{i}")
            self.workers.append(worker)
            self.available_workers.append(worker)
        
        logger.info(f"Worker pool initialized with {num_workers} workers")
    
    def start(self):
        """
        Start all workers.
        Inicia todos los workers.
        """
        for worker in self.workers:
            worker.start()
        
        logger.info(f"Started {len(self.workers)} workers")
    
    def get_available_worker(self) -> Optional[Worker]:
        """
        Get an available worker.
        Obtiene un worker disponible.
        """
        with self._lock:
            if self.available_workers:
                worker = self.available_workers.pop(0)
                
                # Check if worker is alive
                if not worker.is_alive():
                    logger.warning(f"Worker {worker.worker_id} is dead, restarting")
                    worker.start()
                
                return worker
            return None
    
    def return_worker(self, worker: Worker):
        """
        Return worker to available pool.
        Devuelve worker al pool disponible.
        """
        with self._lock:
            if worker not in self.available_workers:
                self.available_workers.append(worker)
    
    def scale_up(self, additional_workers: int) -> int:
        """
        Add more workers to the pool.
        Agrega más workers al pool.
        """
        added = 0
        with self._lock:
            for i in range(additional_workers):
                worker_id = f"worker_{self.num_workers + i}"
                worker = Worker(worker_id=worker_id)
                worker.start()
                
                self.workers.append(worker)
                self.available_workers.append(worker)
                added += 1
            
            self.num_workers += added
        
        logger.info(f"Scaled up pool by {added} workers")
        return added
    
    def scale_down(self, remove_workers: int) -> int:
        """
        Remove workers from the pool.
        Elimina workers del pool.
        """
        removed = 0
        with self._lock:
            for i in range(min(remove_workers, len(self.available_workers))):
                if self.available_workers:
                    worker = self.available_workers.pop()
                    worker.shutdown()
                    
                    if worker in self.workers:
                        self.workers.remove(worker)
                    
                    removed += 1
            
            self.num_workers = len(self.workers)
        
        logger.info(f"Scaled down pool by {removed} workers")
        return removed
    
    def check_health(self):
        """
        Check health of all workers.
        Verifica la salud de todos los workers.
        """
        dead_workers = []
        
        for worker in self.workers:
            if not worker.is_alive():
                dead_workers.append(worker)
                logger.warning(f"Worker {worker.worker_id} is dead")
        
        # Restart dead workers
        for worker in dead_workers:
            logger.info(f"Restarting worker {worker.worker_id}")
            worker.start()
    
    def shutdown(self):
        """
        Shutdown all workers.
        Apaga todos los workers.
        """
        logger.info("Shutting down worker pool")
        
        for worker in self.workers:
            worker.shutdown()
        
        logger.info("Worker pool shutdown complete")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get pool statistics.
        Obtiene estadísticas del pool.
        """
        worker_stats = [worker.get_statistics() for worker in self.workers]
        
        total_tasks = sum(w['tasks_executed'] for w in worker_stats)
        total_succeeded = sum(w['tasks_succeeded'] for w in worker_stats)
        total_failed = sum(w['tasks_failed'] for w in worker_stats)
        total_time = sum(w['total_execution_time'] for w in worker_stats)
        
        return {
            'num_workers': self.num_workers,
            'available_workers': len(self.available_workers),
            'busy_workers': self.num_workers - len(self.available_workers),
            'total_tasks_executed': total_tasks,
            'total_tasks_succeeded': total_succeeded,
            'total_tasks_failed': total_failed,
            'total_execution_time': total_time,
            'average_execution_time': total_time / total_tasks if total_tasks > 0 else 0,
            'worker_details': worker_stats
        }