"""
Fixed Worker implementation for distributed task execution.
Implementación arreglada de worker para ejecución distribuida de tareas.
"""
import os
import time
import threading
import queue
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


def worker_process_main(worker_id: str, task_queue: Queue, result_queue: Queue, 
                        shutdown_event: Event, stats_dict: Dict):
    """
    Main worker process function - completely separate from Worker class.
    Función principal del proceso worker - completamente separada de la clase Worker.
    """
    # Set process name
    try:
        import setproctitle
        setproctitle.setproctitle(f"StreamScale-Worker-{worker_id}")
    except ImportError:
        pass
    
    logger.info(f"Worker {worker_id} loop started in process {os.getpid()}")
    
    # Create local stats object
    local_stats = WorkerStats(worker_id=worker_id)
    
    while True:
        try:
            # Check shutdown event first
            if shutdown_event.is_set():
                logger.info(f"Worker {worker_id} received shutdown signal")
                break
            
            # Get task from queue with timeout
            try:
                task = task_queue.get(timeout=0.5)
            except queue.Empty:
                # Normal timeout, check shutdown and continue
                continue
            
            if task is None:
                # Poison pill received
                logger.info(f"Worker {worker_id} received poison pill")
                break
            
            # Execute task
            logger.info(f"Worker {worker_id} executing task {task.task_id}")
            start_time = time.time()
            
            # Update stats
            local_stats.status = "busy"
            local_stats.current_task = task.task_id
            
            try:
                # Deserialize function if needed
                if not task.func:
                    task.deserialize_function()
                
                # Execute function
                result = task.func(*task.args, **task.kwargs)
                
                execution_time = time.time() - start_time
                
                # Update stats
                local_stats.tasks_executed += 1
                local_stats.tasks_succeeded += 1
                local_stats.total_execution_time += execution_time
                local_stats.last_task_at = datetime.utcnow()
                
                # Create result
                task_result = TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    result=result,
                    execution_time=execution_time,
                    worker_id=worker_id
                )
                
                logger.info(f"Worker {worker_id} completed task {task.task_id}")
                
            except Exception as e:
                execution_time = time.time() - start_time
                
                # Update stats
                local_stats.tasks_executed += 1
                local_stats.tasks_failed += 1
                local_stats.total_execution_time += execution_time
                
                # Create error result
                task_result = TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    error=str(e),
                    execution_time=execution_time,
                    worker_id=worker_id
                )
                
                logger.error(f"Worker {worker_id} failed task {task.task_id}: {str(e)}")
            
            finally:
                # Reset status
                local_stats.status = "idle"
                local_stats.current_task = None
                
                # Update shared stats dict
                stats_dict[worker_id] = local_stats.to_dict()
            
            # Put result in queue
            result_queue.put(task_result)
            
        except queue.Empty:
            # This should not happen as we handle it above
            continue
        except (KeyboardInterrupt, SystemExit):
            # Clean exit on interrupt
            logger.info(f"Worker {worker_id} interrupted")
            break
        except Exception as e:
            # Only log errors if not shutting down
            if not shutdown_event.is_set():
                logger.error(f"Worker {worker_id} error: {str(e)}", exc_info=True)
            else:
                # During shutdown, just exit cleanly
                break
    
    logger.info(f"Worker {worker_id} loop stopped")


class Worker:
    """
    Worker process for task execution - Fixed version without threading.Lock in __init__.
    Proceso worker para ejecución de tareas - Versión arreglada sin threading.Lock en __init__.
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
        
        # Use multiprocessing Manager for shared state
        manager = mp.Manager()
        self.task_queue = manager.Queue()
        self.result_queue = manager.Queue()
        self.shutdown_event = manager.Event()
        self.stats_dict = manager.dict()
        
        self.is_available = True
        self._lock = None  # Will be created lazily in main process only
        
        logger.info(f"Worker {self.worker_id} initialized")
    
    def _ensure_lock(self):
        """Ensure lock exists (lazy creation in main process only)"""
        if self._lock is None:
            self._lock = threading.Lock()
    
    def start(self):
        """
        Start worker process.
        Inicia proceso worker.
        """
        if self.process and self.process.is_alive():
            logger.warning(f"Worker {self.worker_id} already running")
            return
        
        # Start process with standalone function
        self.process = Process(
            target=worker_process_main,
            args=(self.worker_id, self.task_queue, self.result_queue, 
                  self.shutdown_event, self.stats_dict),
            name=f"Worker-{self.worker_id}"
        )
        self.process.daemon = True
        self.process.start()
        
        logger.info(f"Worker {self.worker_id} started")
    
    def submit_task(self, task: Task) -> bool:
        """
        Submit task to worker.
        Envía tarea al worker.
        """
        self._ensure_lock()
        
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
            self._ensure_lock()
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
        
        # Set shutdown event first
        try:
            self.shutdown_event.set()
        except Exception as e:
            logger.debug(f"Error setting shutdown event: {e}")
        
        # Send poison pill to signal worker to exit
        try:
            self.task_queue.put(None, timeout=1)
        except Exception as e:
            logger.debug(f"Error sending poison pill: {e}")
        
        # Wait for process to finish gracefully
        if self.process and self.process.is_alive():
            try:
                # Give it time to finish current task
                logger.info(f"Waiting for worker {self.worker_id} to finish...")
                self.process.join(timeout=3)
                
                # If still alive, terminate it
                if self.process.is_alive():
                    logger.warning(f"Force terminating worker {self.worker_id}")
                    self.process.terminate()
                    # Give it a moment to terminate
                    self.process.join(timeout=1)
                    
                    # Last resort: kill it
                    if self.process.is_alive():
                        logger.error(f"Force killing worker {self.worker_id}")
                        self.process.kill()
                        self.process.join(timeout=1)
            except Exception as e:
                logger.debug(f"Error during worker shutdown: {e}")
        
        logger.info(f"Worker {self.worker_id} shutdown complete")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get worker statistics.
        Obtiene estadísticas del worker.
        """
        # Get stats from shared dict if available
        if self.worker_id in self.stats_dict:
            return self.stats_dict[self.worker_id]
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
        self._lock = None  # Will be created lazily
        
        # Create workers
        for i in range(num_workers):
            worker = Worker(worker_id=f"worker_{i}")
            self.workers.append(worker)
            self.available_workers.append(worker)
        
        logger.info(f"Worker pool initialized with {num_workers} workers")
    
    def _ensure_lock(self):
        """Ensure lock exists (lazy creation)"""
        if self._lock is None:
            self._lock = threading.Lock()
    
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
        self._ensure_lock()
        
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
        self._ensure_lock()
        
        with self._lock:
            if worker not in self.available_workers:
                self.available_workers.append(worker)
    
    def scale(self, new_size: int):
        """
        Scale worker pool to new size.
        Escala el pool de workers a nuevo tamaño.
        """
        self._ensure_lock()
        
        with self._lock:
            current_size = len(self.workers)
            
            if new_size > current_size:
                # Add workers
                for i in range(current_size, new_size):
                    worker = Worker(worker_id=f"worker_{i}")
                    worker.start()
                    self.workers.append(worker)
                    self.available_workers.append(worker)
                
                logger.info(f"Scaled up from {current_size} to {new_size} workers")
                
            elif new_size < current_size:
                # Remove workers
                workers_to_remove = self.workers[new_size:]
                self.workers = self.workers[:new_size]
                
                # Remove from available pool
                for worker in workers_to_remove:
                    if worker in self.available_workers:
                        self.available_workers.remove(worker)
                    worker.shutdown()
                
                logger.info(f"Scaled down from {current_size} to {new_size} workers")
            
            # Update num_workers attribute
            self.num_workers = len(self.workers)
    
    def scale_up(self, num_workers: int) -> int:
        """
        Scale up by adding specified number of workers.
        Escala agregando el número especificado de workers.
        
        Args:
            num_workers: Number of workers to add
            
        Returns:
            Number of workers actually added
        """
        self._ensure_lock()
        
        with self._lock:
            current_size = len(self.workers)
            new_size = current_size + num_workers
            
            # Add workers
            for i in range(current_size, new_size):
                worker = Worker(worker_id=f"worker_{i}")
                worker.start()
                self.workers.append(worker)
                self.available_workers.append(worker)
            
            # Update num_workers attribute
            self.num_workers = len(self.workers)
            
            logger.info(f"Scaled up by {num_workers} workers (from {current_size} to {new_size})")
            return num_workers
    
    def scale_down(self, num_workers: int) -> int:
        """
        Scale down by removing specified number of workers.
        Escala removiendo el número especificado de workers.
        
        Args:
            num_workers: Number of workers to remove
            
        Returns:
            Number of workers actually removed
        """
        self._ensure_lock()
        
        with self._lock:
            current_size = len(self.workers)
            workers_to_remove_count = min(num_workers, current_size)
            
            if workers_to_remove_count == 0:
                return 0
            
            # Remove workers from the end
            workers_to_remove = self.workers[-workers_to_remove_count:]
            self.workers = self.workers[:-workers_to_remove_count]
            
            # Remove from available pool and shutdown
            for worker in workers_to_remove:
                if worker in self.available_workers:
                    self.available_workers.remove(worker)
                worker.shutdown()
            
            # Update num_workers attribute
            self.num_workers = len(self.workers)
            new_size = self.num_workers
            
            logger.info(f"Scaled down by {workers_to_remove_count} workers (from {current_size} to {new_size})")
            return workers_to_remove_count
    
    def check_health(self):
        """
        Check health of all workers and restart dead ones.
        Verifica la salud de todos los workers y reinicia los muertos.
        """
        self._ensure_lock()
        
        with self._lock:
            for worker in self.workers:
                if not worker.is_alive():
                    logger.warning(f"Worker {worker.worker_id} is dead, restarting")
                    worker.start()
                    
                    # Ensure it's in available pool if it was there before
                    if worker not in self.available_workers:
                        self.available_workers.append(worker)
    
    def shutdown(self):
        """
        Shutdown all workers gracefully.
        Apaga todos los workers ordenadamente.
        """
        logger.info(f"Shutting down {len(self.workers)} workers")
        
        # First, signal all workers to shutdown
        for worker in self.workers:
            try:
                worker.shutdown_event.set()
            except Exception as e:
                logger.debug(f"Error setting shutdown event for worker: {e}")
        
        # Then send poison pills to all workers
        for worker in self.workers:
            try:
                worker.task_queue.put(None, timeout=0.5)
            except Exception as e:
                logger.debug(f"Error sending poison pill to worker: {e}")
        
        # Finally, wait for all workers to finish
        for worker in self.workers:
            try:
                worker.shutdown()
            except Exception as e:
                logger.debug(f"Error shutting down worker: {e}")
        
        # Clear the lists
        self.workers.clear()
        self.available_workers.clear()
        
        logger.info("All workers shutdown complete")
    
    def get_statistics(self) -> List[Dict[str, Any]]:
        """
        Get statistics for all workers.
        Obtiene estadísticas de todos los workers.
        """
        return [worker.get_statistics() for worker in self.workers]