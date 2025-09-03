"""
Distributed task scheduler with Redis and RabbitMQ integration.
Planificador de tareas distribuido con integración Redis y RabbitMQ.
"""
import time
import json
import threading
import multiprocessing as mp
from multiprocessing import Process, Queue, Manager, Event
from typing import Dict, List, Optional, Callable, Any, Set
from datetime import datetime, timedelta
from queue import PriorityQueue, Empty
import redis
import pika
import signal
import sys

from src.scheduler.task import Task, TaskStatus, TaskPriority, TaskResult, TaskGraph
from src.scheduler.worker import Worker, WorkerPool
from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


class DistributedScheduler:
    """
    Distributed task scheduler with worker pool management.
    Planificador de tareas distribuido con gestión de pool de workers.
    """
    
    def __init__(self,
                 num_workers: int = 4,
                 max_queue_size: int = 1000,
                 enable_monitoring: bool = True,
                 use_redis: bool = True,
                 use_rabbitmq: bool = True):
        """
        Initialize distributed scheduler.
        
        Args:
            num_workers: Number of worker processes
            max_queue_size: Maximum size of task queue
            enable_monitoring: Enable monitoring thread
            use_redis: Use Redis for priority queue
            use_rabbitmq: Use RabbitMQ for task distribution
        """
        self.num_workers = num_workers
        self.max_queue_size = max_queue_size
        self.enable_monitoring = enable_monitoring
        self.use_redis = use_redis
        self.use_rabbitmq = use_rabbitmq
        
        # Task management
        self.task_graph = TaskGraph()
        self.local_queue = PriorityQueue(maxsize=max_queue_size)
        
        # Worker management
        self.worker_pool = WorkerPool(num_workers)
        self.workers: Dict[str, Worker] = {}
        
        # Process management
        self.manager = Manager()
        self.shared_state = self.manager.dict()
        self.shutdown_event = Event()
        
        # External connections
        self.redis_client: Optional[redis.Redis] = None
        self.rabbitmq_connection: Optional[pika.BlockingConnection] = None
        self.rabbitmq_channel: Optional[pika.channel.Channel] = None
        
        # Monitoring
        self.monitor_thread: Optional[threading.Thread] = None
        self.stats = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'tasks_timeout': 0,
            'tasks_cancelled': 0,
            'total_execution_time': 0.0,
            'start_time': datetime.utcnow()
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals / Maneja señales de apagado"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
        sys.exit(0)
    
    def initialize(self):
        """
        Initialize scheduler components.
        Inicializa componentes del planificador.
        """
        logger.info("Initializing distributed scheduler")
        
        # Initialize Redis
        if self.use_redis:
            try:
                self.redis_client = redis.Redis(
                    host=settings.redis.host,
                    port=settings.redis.port,
                    db=settings.redis.db,
                    decode_responses=True
                )
                self.redis_client.ping()
                logger.info("Redis connection established")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {str(e)}")
                self.use_redis = False
        
        # Initialize RabbitMQ
        if self.use_rabbitmq:
            try:
                connection_params = pika.ConnectionParameters(
                    host=settings.rabbitmq.host,
                    port=settings.rabbitmq.port,
                    credentials=pika.PlainCredentials(
                        settings.rabbitmq.user,
                        settings.rabbitmq.password
                    ),
                    virtual_host=settings.rabbitmq.vhost
                )
                self.rabbitmq_connection = pika.BlockingConnection(connection_params)
                self.rabbitmq_channel = self.rabbitmq_connection.channel()
                
                # Declare task queue
                self.rabbitmq_channel.queue_declare(
                    queue='task_queue',
                    durable=True
                )
                
                # Declare result queue
                self.rabbitmq_channel.queue_declare(
                    queue='result_queue',
                    durable=True
                )
                
                logger.info("RabbitMQ connection established")
            except Exception as e:
                logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
                self.use_rabbitmq = False
        
        # Initialize worker pool
        self.worker_pool.start()
        
        # Start monitoring thread
        if self.enable_monitoring:
            self.monitor_thread = threading.Thread(
                target=self._monitoring_loop,
                daemon=True
            )
            self.monitor_thread.start()
            logger.info("Monitoring thread started")
        
        logger.info(f"Scheduler initialized with {self.num_workers} workers")
    
    def submit_task(self, 
                   func: Callable,
                   args: tuple = (),
                   kwargs: Dict[str, Any] = None,
                   name: Optional[str] = None,
                   priority: TaskPriority = TaskPriority.NORMAL,
                   dependencies: Optional[Set[str]] = None,
                   timeout: Optional[float] = None) -> str:
        """
        Submit a task for execution.
        Envía una tarea para ejecución.
        
        Returns:
            Task ID
        """
        task = Task(
            name=name or func.__name__,
            func=func,
            args=args,
            kwargs=kwargs or {},
            priority=priority,
            dependencies=dependencies or set(),
            timeout=timeout or settings.scheduler.task_timeout
        )
        
        # Add to task graph
        self.task_graph.add_task(task)
        
        # Queue task
        self._queue_task(task)
        
        # Update stats
        self.stats['tasks_submitted'] += 1
        
        logger.info(f"Task {task.task_id} submitted: {task.name}")
        return task.task_id
    
    def _queue_task(self, task: Task):
        """
        Queue task for execution.
        Encola tarea para ejecución.
        """
        # Check if task is ready
        if not task.is_ready(self.task_graph.completed):
            logger.debug(f"Task {task.task_id} waiting for dependencies")
            return
        
        task.status = TaskStatus.QUEUED
        task.scheduled_at = datetime.utcnow()
        
        # Add to Redis priority queue if available
        if self.use_redis and self.redis_client:
            try:
                # Use sorted set for priority queue
                score = task.priority.value * 1000000 + time.time()
                self.redis_client.zadd(
                    'task_queue',
                    {task.to_json(): score}
                )
                logger.debug(f"Task {task.task_id} added to Redis queue")
            except Exception as e:
                logger.error(f"Failed to add task to Redis: {str(e)}")
                self._queue_local(task)
        else:
            self._queue_local(task)
        
        # Publish to RabbitMQ if available
        if self.use_rabbitmq and self.rabbitmq_channel:
            try:
                self.rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key='task_queue',
                    body=task.to_json(),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Make message persistent
                        priority=task.priority.value
                    )
                )
                logger.debug(f"Task {task.task_id} published to RabbitMQ")
            except Exception as e:
                logger.error(f"Failed to publish task to RabbitMQ: {str(e)}")
    
    def _queue_local(self, task: Task):
        """
        Queue task locally.
        Encola tarea localmente.
        """
        try:
            self.local_queue.put_nowait((task.priority.value, task))
        except Exception as e:
            logger.error(f"Failed to queue task locally: {str(e)}")
    
    def _get_next_task(self) -> Optional[Task]:
        """
        Get next task from queue.
        Obtiene siguiente tarea de la cola.
        """
        task = None
        
        # Try Redis first
        if self.use_redis and self.redis_client:
            try:
                # Get highest priority task
                result = self.redis_client.zrange('task_queue', 0, 0)
                if result:
                    task_json = result[0]
                    self.redis_client.zrem('task_queue', task_json)
                    task = Task.from_json(task_json)
                    logger.debug(f"Got task {task.task_id} from Redis")
            except Exception as e:
                logger.error(f"Failed to get task from Redis: {str(e)}")
        
        # Fallback to local queue
        if not task:
            try:
                priority, task = self.local_queue.get_nowait()
                logger.debug(f"Got task {task.task_id} from local queue")
            except Empty:
                pass
        
        return task
    
    def _execute_task(self, task: Task, worker_id: str) -> TaskResult:
        """
        Execute a task on a worker.
        Ejecuta una tarea en un worker.
        """
        logger.info(f"Executing task {task.task_id} on worker {worker_id}")
        
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        task.assigned_worker = worker_id
        
        start_time = time.time()
        
        try:
            # Deserialize function if needed
            if not task.func:
                task.deserialize_function()
            
            # Execute with timeout
            if task.timeout:
                # Use multiprocessing with timeout
                result = self._execute_with_timeout(
                    task.func,
                    task.args,
                    task.kwargs,
                    task.timeout
                )
            else:
                result = task.func(*task.args, **task.kwargs)
            
            execution_time = time.time() - start_time
            
            # Create successful result
            task_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                result=result,
                execution_time=execution_time,
                worker_id=worker_id,
                completed_at=datetime.utcnow()
            )
            
            # Update task and graph
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.result = task_result
            self.task_graph.mark_completed(task.task_id)
            
            # Update stats
            self.stats['tasks_completed'] += 1
            self.stats['total_execution_time'] += execution_time
            
            logger.info(f"Task {task.task_id} completed successfully")
            
            # Check for dependent tasks
            self._process_dependent_tasks()
            
        except TimeoutError:
            execution_time = time.time() - start_time
            
            task_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.TIMEOUT,
                error="Task execution timed out",
                execution_time=execution_time,
                worker_id=worker_id
            )
            
            task.status = TaskStatus.TIMEOUT
            task.result = task_result
            self.stats['tasks_timeout'] += 1
            
            logger.warning(f"Task {task.task_id} timed out")
            
            # Check for retry
            if task.should_retry():
                self._retry_task(task)
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            task_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=str(e),
                execution_time=execution_time,
                worker_id=worker_id
            )
            
            task.status = TaskStatus.FAILED
            task.result = task_result
            self.task_graph.mark_failed(task.task_id)
            self.stats['tasks_failed'] += 1
            
            logger.error(f"Task {task.task_id} failed: {str(e)}")
            
            # Check for retry
            if task.should_retry():
                self._retry_task(task)
        
        # Store result if using external storage
        self._store_result(task_result)
        
        return task_result
    
    def _execute_with_timeout(self, func: Callable, args: tuple, 
                            kwargs: Dict, timeout: float) -> Any:
        """
        Execute function with timeout.
        Ejecuta función con timeout.
        """
        from multiprocessing import Process, Queue
        
        result_queue = Queue()
        
        def wrapper():
            try:
                result = func(*args, **kwargs)
                result_queue.put(result)
            except Exception as e:
                result_queue.put(e)
        
        process = Process(target=wrapper)
        process.start()
        process.join(timeout)
        
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeoutError(f"Task execution exceeded {timeout} seconds")
        
        if not result_queue.empty():
            result = result_queue.get()
            if isinstance(result, Exception):
                raise result
            return result
        
        return None
    
    def _retry_task(self, task: Task):
        """
        Retry a failed task.
        Reintenta una tarea fallida.
        """
        task.retry_count += 1
        task.status = TaskStatus.PENDING
        
        logger.info(f"Retrying task {task.task_id} (attempt {task.retry_count})")
        
        # Add delay before retry
        time.sleep(task.retry_delay)
        
        # Re-queue task
        self._queue_task(task)
    
    def _process_dependent_tasks(self):
        """
        Process tasks that have had dependencies satisfied.
        Procesa tareas cuyas dependencias han sido satisfechas.
        """
        ready_tasks = self.task_graph.get_ready_tasks()
        
        for task in ready_tasks:
            if task.status == TaskStatus.PENDING:
                self._queue_task(task)
    
    def _store_result(self, result: TaskResult):
        """
        Store task result in external storage.
        Almacena resultado de tarea en almacenamiento externo.
        """
        # Store in Redis
        if self.use_redis and self.redis_client:
            try:
                result_key = f"task_result:{result.task_id}"
                self.redis_client.setex(
                    result_key,
                    86400,  # 24 hour TTL
                    json.dumps(result.to_dict(), default=str)
                )
            except Exception as e:
                logger.error(f"Failed to store result in Redis: {str(e)}")
        
        # Publish to RabbitMQ
        if self.use_rabbitmq and self.rabbitmq_channel:
            try:
                self.rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key='result_queue',
                    body=json.dumps(result.to_dict(), default=str),
                    properties=pika.BasicProperties(
                        delivery_mode=2  # Persistent
                    )
                )
            except Exception as e:
                logger.error(f"Failed to publish result to RabbitMQ: {str(e)}")
    
    def run(self):
        """
        Main scheduler loop.
        Bucle principal del planificador.
        """
        logger.info("Starting scheduler main loop")
        
        while not self.shutdown_event.is_set():
            try:
                # Get next task
                task = self._get_next_task()
                
                if task:
                    # Assign to available worker
                    worker = self.worker_pool.get_available_worker()
                    
                    if worker:
                        # Execute task
                        result = self._execute_task(task, worker.worker_id)
                        
                        # Return worker to pool
                        self.worker_pool.return_worker(worker)
                    else:
                        # No workers available, re-queue
                        self._queue_task(task)
                        time.sleep(0.1)
                else:
                    # No tasks available
                    time.sleep(0.1)
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(1)
        
        logger.info("Scheduler main loop stopped")
    
    def _monitoring_loop(self):
        """
        Monitoring loop for statistics and health checks.
        Bucle de monitoreo para estadísticas y verificaciones de salud.
        """
        logger.info("Starting monitoring loop")
        
        while not self.shutdown_event.is_set():
            try:
                # Collect statistics
                stats = self.get_statistics()
                
                # Log statistics
                logger.info(f"Scheduler statistics: {stats}")
                
                # Store in Redis
                if self.use_redis and self.redis_client:
                    self.redis_client.setex(
                        'scheduler:stats',
                        60,
                        json.dumps(stats, default=str)
                    )
                
                # Check worker health
                self.worker_pool.check_health()
                
                # Check for stuck tasks
                self._check_stuck_tasks()
                
                # Sleep
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(10)
        
        logger.info("Monitoring loop stopped")
    
    def _check_stuck_tasks(self):
        """
        Check for stuck or timed out tasks.
        Verifica tareas atascadas o expiradas.
        """
        for task in self.task_graph.tasks.values():
            if task.status == TaskStatus.RUNNING and task.is_timeout():
                logger.warning(f"Task {task.task_id} appears stuck, marking as timeout")
                task.status = TaskStatus.TIMEOUT
                self.stats['tasks_timeout'] += 1
                
                if task.should_retry():
                    self._retry_task(task)
    
    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a task.
        Cancela una tarea.
        """
        if task_id in self.task_graph.tasks:
            task = self.task_graph.tasks[task_id]
            
            if task.status in [TaskStatus.PENDING, TaskStatus.QUEUED]:
                task.status = TaskStatus.CANCELLED
                self.stats['tasks_cancelled'] += 1
                logger.info(f"Task {task_id} cancelled")
                return True
            else:
                logger.warning(f"Cannot cancel task {task_id} with status {task.status}")
                return False
        
        logger.error(f"Task {task_id} not found")
        return False
    
    def scale_workers(self, target_workers: int):
        """
        Dynamically scale worker pool.
        Escala dinámicamente el pool de workers.
        """
        current_workers = self.worker_pool.num_workers
        
        if target_workers > current_workers:
            # Scale up
            added = self.worker_pool.scale_up(target_workers - current_workers)
            logger.info(f"Scaled up by {added} workers")
        elif target_workers < current_workers:
            # Scale down
            removed = self.worker_pool.scale_down(current_workers - target_workers)
            logger.info(f"Scaled down by {removed} workers")
        else:
            logger.info("No scaling needed")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get scheduler statistics.
        Obtiene estadísticas del planificador.
        """
        runtime = (datetime.utcnow() - self.stats['start_time']).total_seconds()
        
        return {
            **self.stats,
            'runtime_seconds': runtime,
            'tasks_per_second': self.stats['tasks_completed'] / runtime if runtime > 0 else 0,
            'average_execution_time': (
                self.stats['total_execution_time'] / self.stats['tasks_completed']
                if self.stats['tasks_completed'] > 0 else 0
            ),
            'worker_stats': self.worker_pool.get_statistics(),
            'graph_stats': self.task_graph.get_statistics(),
            'queue_size': self.local_queue.qsize() if hasattr(self.local_queue, 'qsize') else 0
        }
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a specific task.
        Obtiene estado de una tarea específica.
        """
        if task_id in self.task_graph.tasks:
            task = self.task_graph.tasks[task_id]
            return task.to_dict()
        
        # Check Redis for result
        if self.use_redis and self.redis_client:
            try:
                result_key = f"task_result:{task_id}"
                result = self.redis_client.get(result_key)
                if result:
                    return json.loads(result)
            except Exception as e:
                logger.error(f"Failed to get task status from Redis: {str(e)}")
        
        return None
    
    def shutdown(self):
        """
        Graceful shutdown of the scheduler.
        Apagado ordenado del planificador.
        """
        logger.info("Shutting down scheduler...")
        
        # Set shutdown event
        self.shutdown_event.set()
        
        # Stop worker pool
        self.worker_pool.shutdown()
        
        # Close external connections
        if self.redis_client:
            self.redis_client.close()
        
        if self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
            self.rabbitmq_connection.close()
        
        logger.info("Scheduler shutdown complete")