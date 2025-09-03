"""
Custom context manager for resource management.
Gestor de contexto personalizado para gestión de recursos.
"""
import time
import asyncio
from typing import Dict, Any, List, Optional, Type, Callable
from contextlib import contextmanager, asynccontextmanager, ExitStack, AsyncExitStack
from dataclasses import dataclass, field
from datetime import datetime
import traceback
import threading
from collections import defaultdict
import psycopg2
import redis
import pika
import httpx

from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ResourceMetrics:
    """
    Metrics for resource usage.
    Métricas para uso de recursos.
    """
    resource_name: str
    acquisition_time: float = 0.0
    release_time: float = 0.0
    usage_duration: float = 0.0
    error_count: int = 0
    success_count: int = 0
    last_error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary / Convertir a diccionario"""
        return {
            "resource_name": self.resource_name,
            "acquisition_time_ms": self.acquisition_time * 1000,
            "release_time_ms": self.release_time * 1000,
            "usage_duration_ms": self.usage_duration * 1000,
            "error_count": self.error_count,
            "success_count": self.success_count,
            "last_error": self.last_error,
            "created_at": self.created_at.isoformat()
        }


class ResourceManager:
    """
    Robust context manager for managing multiple external resources.
    Gestor de contexto robusto para gestionar múltiples recursos externos.
    """
    
    def __init__(self, 
                 enable_metrics: bool = True,
                 enable_logging: bool = True,
                 retry_attempts: int = 3,
                 retry_delay: float = 1.0):
        """
        Initialize the resource manager.
        
        Args:
            enable_metrics: Enable performance metrics collection
            enable_logging: Enable detailed logging
            retry_attempts: Number of retry attempts for resource acquisition
            retry_delay: Delay between retry attempts in seconds
        """
        self.enable_metrics = enable_metrics
        self.enable_logging = enable_logging
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        # Resource tracking
        self.resources: Dict[str, Any] = {}
        self.metrics: Dict[str, ResourceMetrics] = {}
        self.cleanup_stack = ExitStack()
        self._lock = threading.Lock()
        
        # Nested context tracking
        self.nested_managers: List['ResourceManager'] = []
        self.parent_manager: Optional['ResourceManager'] = None
        
        if self.enable_logging:
            logger.info("ResourceManager initialized", 
                       metrics_enabled=enable_metrics,
                       retry_attempts=retry_attempts)
    
    def __enter__(self):
        """Enter the context / Entrar al contexto"""
        self._acquisition_start = time.time()
        
        if self.enable_logging:
            logger.info("Entering ResourceManager context")
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context and cleanup resources.
        Salir del contexto y limpiar recursos.
        """
        cleanup_errors = []
        
        # Log exception if occurred
        if exc_val:
            logger.error("Exception in ResourceManager context",
                        error_type=exc_type.__name__ if exc_type else None,
                        error_message=str(exc_val),
                        traceback=traceback.format_tb(exc_tb))
        
        # Cleanup nested managers first
        for nested in reversed(self.nested_managers):
            try:
                nested.__exit__(None, None, None)
            except Exception as e:
                cleanup_errors.append(f"Nested manager cleanup: {str(e)}")
        
        # Cleanup resources in reverse order
        for resource_name in reversed(list(self.resources.keys())):
            try:
                self._cleanup_resource(resource_name)
            except Exception as e:
                cleanup_errors.append(f"{resource_name}: {str(e)}")
        
        # Close cleanup stack
        try:
            self.cleanup_stack.__exit__(exc_type, exc_val, exc_tb)
        except Exception as e:
            cleanup_errors.append(f"Cleanup stack: {str(e)}")
        
        # Update metrics
        if self.enable_metrics:
            total_duration = time.time() - self._acquisition_start
            for metrics in self.metrics.values():
                metrics.usage_duration = total_duration
        
        # Log cleanup errors
        if cleanup_errors:
            logger.error("Errors during resource cleanup", errors=cleanup_errors)
        
        if self.enable_logging:
            logger.info("Exited ResourceManager context",
                       duration_ms=(time.time() - self._acquisition_start) * 1000,
                       resources_cleaned=len(self.resources))
        
        # Don't suppress the original exception
        return False
    
    def acquire_database(self, 
                        name: str = "default",
                        **connection_params) -> Any:
        """
        Acquire a database connection.
        Adquiere una conexión a base de datos.
        """
        start_time = time.time()
        
        # Use provided params or defaults from settings
        params = {
            'host': settings.database.host,
            'port': settings.database.port,
            'database': settings.database.database,
            'user': settings.database.user,
            'password': settings.database.password,
            **connection_params
        }
        
        connection = None
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                if self.enable_logging:
                    logger.info(f"Acquiring database connection '{name}' (attempt {attempt + 1})")
                
                connection = psycopg2.connect(**params)
                connection.autocommit = False
                
                # Test connection
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                
                # Store resource
                with self._lock:
                    self.resources[f"db_{name}"] = connection
                    
                    # Track metrics
                    if self.enable_metrics:
                        if f"db_{name}" not in self.metrics:
                            self.metrics[f"db_{name}"] = ResourceMetrics(f"db_{name}")
                        self.metrics[f"db_{name}"].acquisition_time = time.time() - start_time
                        self.metrics[f"db_{name}"].success_count += 1
                
                if self.enable_logging:
                    logger.info(f"Database connection '{name}' acquired successfully")
                
                return connection
                
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Failed to acquire database connection (attempt {attempt + 1})",
                             error=last_error)
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
                
        # All retries failed
        if self.enable_metrics and f"db_{name}" in self.metrics:
            self.metrics[f"db_{name}"].error_count += 1
            self.metrics[f"db_{name}"].last_error = last_error
        
        raise ConnectionError(f"Failed to acquire database connection '{name}' after {self.retry_attempts} attempts: {last_error}")
    
    def acquire_redis(self,
                     name: str = "default",
                     **connection_params) -> redis.Redis:
        """
        Acquire a Redis connection.
        Adquiere una conexión Redis.
        """
        start_time = time.time()
        
        # Use provided params or defaults
        params = {
            'host': settings.redis.host,
            'port': settings.redis.port,
            'db': settings.redis.db,
            'decode_responses': True,
            **connection_params
        }
        
        client = None
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                if self.enable_logging:
                    logger.info(f"Acquiring Redis connection '{name}' (attempt {attempt + 1})")
                
                client = redis.Redis(**params)
                
                # Test connection
                client.ping()
                
                # Store resource
                with self._lock:
                    self.resources[f"redis_{name}"] = client
                    
                    # Track metrics
                    if self.enable_metrics:
                        if f"redis_{name}" not in self.metrics:
                            self.metrics[f"redis_{name}"] = ResourceMetrics(f"redis_{name}")
                        self.metrics[f"redis_{name}"].acquisition_time = time.time() - start_time
                        self.metrics[f"redis_{name}"].success_count += 1
                
                if self.enable_logging:
                    logger.info(f"Redis connection '{name}' acquired successfully")
                
                return client
                
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Failed to acquire Redis connection (attempt {attempt + 1})",
                             error=last_error)
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
        
        # All retries failed
        if self.enable_metrics and f"redis_{name}" in self.metrics:
            self.metrics[f"redis_{name}"].error_count += 1
            self.metrics[f"redis_{name}"].last_error = last_error
        
        raise ConnectionError(f"Failed to acquire Redis connection '{name}' after {self.retry_attempts} attempts: {last_error}")
    
    def acquire_rabbitmq(self,
                        name: str = "default",
                        **connection_params) -> pika.BlockingConnection:
        """
        Acquire a RabbitMQ connection.
        Adquiere una conexión RabbitMQ.
        """
        start_time = time.time()
        
        # Use provided params or defaults
        params = pika.ConnectionParameters(
            host=connection_params.get('host', settings.rabbitmq.host),
            port=connection_params.get('port', settings.rabbitmq.port),
            credentials=pika.PlainCredentials(
                connection_params.get('username', settings.rabbitmq.user),
                connection_params.get('password', settings.rabbitmq.password)
            ),
            virtual_host=connection_params.get('vhost', settings.rabbitmq.vhost)
        )
        
        connection = None
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                if self.enable_logging:
                    logger.info(f"Acquiring RabbitMQ connection '{name}' (attempt {attempt + 1})")
                
                connection = pika.BlockingConnection(params)
                
                # Store resource
                with self._lock:
                    self.resources[f"rabbitmq_{name}"] = connection
                    
                    # Track metrics
                    if self.enable_metrics:
                        if f"rabbitmq_{name}" not in self.metrics:
                            self.metrics[f"rabbitmq_{name}"] = ResourceMetrics(f"rabbitmq_{name}")
                        self.metrics[f"rabbitmq_{name}"].acquisition_time = time.time() - start_time
                        self.metrics[f"rabbitmq_{name}"].success_count += 1
                
                if self.enable_logging:
                    logger.info(f"RabbitMQ connection '{name}' acquired successfully")
                
                return connection
                
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Failed to acquire RabbitMQ connection (attempt {attempt + 1})",
                             error=last_error)
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
        
        # All retries failed
        if self.enable_metrics and f"rabbitmq_{name}" in self.metrics:
            self.metrics[f"rabbitmq_{name}"].error_count += 1
            self.metrics[f"rabbitmq_{name}"].last_error = last_error
        
        raise ConnectionError(f"Failed to acquire RabbitMQ connection '{name}' after {self.retry_attempts} attempts: {last_error}")
    
    def acquire_http_client(self,
                           name: str = "default",
                           base_url: Optional[str] = None,
                           **client_params) -> httpx.Client:
        """
        Acquire an HTTP client.
        Adquiere un cliente HTTP.
        """
        start_time = time.time()
        
        try:
            if self.enable_logging:
                logger.info(f"Acquiring HTTP client '{name}'")
            
            # Create HTTP client
            client = httpx.Client(base_url=base_url, **client_params)
            
            # Store resource
            with self._lock:
                self.resources[f"http_{name}"] = client
                
                # Track metrics
                if self.enable_metrics:
                    if f"http_{name}" not in self.metrics:
                        self.metrics[f"http_{name}"] = ResourceMetrics(f"http_{name}")
                    self.metrics[f"http_{name}"].acquisition_time = time.time() - start_time
                    self.metrics[f"http_{name}"].success_count += 1
            
            if self.enable_logging:
                logger.info(f"HTTP client '{name}' acquired successfully")
            
            return client
            
        except Exception as e:
            if self.enable_metrics and f"http_{name}" in self.metrics:
                self.metrics[f"http_{name}"].error_count += 1
                self.metrics[f"http_{name}"].last_error = str(e)
            raise
    
    def nest(self, child_manager: 'ResourceManager'):
        """
        Nest another resource manager.
        Anida otro gestor de recursos.
        """
        child_manager.parent_manager = self
        self.nested_managers.append(child_manager)
        return child_manager
    
    def _cleanup_resource(self, resource_name: str):
        """
        Clean up a specific resource.
        Limpia un recurso específico.
        """
        start_time = time.time()
        
        try:
            resource = self.resources.get(resource_name)
            if not resource:
                return
            
            if self.enable_logging:
                logger.info(f"Cleaning up resource: {resource_name}")
            
            # Cleanup based on resource type
            if resource_name.startswith("db_"):
                if hasattr(resource, 'close'):
                    resource.close()
            
            elif resource_name.startswith("redis_"):
                if hasattr(resource, 'close'):
                    resource.close()
            
            elif resource_name.startswith("rabbitmq_"):
                if hasattr(resource, 'close'):
                    resource.close()
            
            elif resource_name.startswith("http_"):
                if hasattr(resource, 'close'):
                    resource.close()
            
            # Update metrics
            if self.enable_metrics and resource_name in self.metrics:
                self.metrics[resource_name].release_time = time.time() - start_time
            
            # Remove from resources
            del self.resources[resource_name]
            
            if self.enable_logging:
                logger.info(f"Resource {resource_name} cleaned up successfully")
                
        except Exception as e:
            logger.error(f"Error cleaning up resource {resource_name}", error=str(e))
            if self.enable_metrics and resource_name in self.metrics:
                self.metrics[resource_name].error_count += 1
                self.metrics[resource_name].last_error = str(e)
            raise
    
    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance metrics for all resources.
        Obtiene métricas de rendimiento para todos los recursos.
        """
        return {name: metrics.to_dict() for name, metrics in self.metrics.items()}
    
    def get_resource(self, resource_type: str, name: str = "default") -> Optional[Any]:
        """
        Get an acquired resource.
        Obtiene un recurso adquirido.
        """
        resource_key = f"{resource_type}_{name}"
        return self.resources.get(resource_key)


class AsyncResourceManager:
    """
    Async version of the resource manager.
    Versión asíncrona del gestor de recursos.
    """
    
    def __init__(self,
                 enable_metrics: bool = True,
                 enable_logging: bool = True,
                 retry_attempts: int = 3,
                 retry_delay: float = 1.0):
        """Initialize async resource manager"""
        self.enable_metrics = enable_metrics
        self.enable_logging = enable_logging
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        self.resources: Dict[str, Any] = {}
        self.metrics: Dict[str, ResourceMetrics] = {}
        self.cleanup_stack = AsyncExitStack()
        self._lock = asyncio.Lock()
        
        self.nested_managers: List['AsyncResourceManager'] = []
        self.parent_manager: Optional['AsyncResourceManager'] = None
    
    async def __aenter__(self):
        """Async enter / Entrada asíncrona"""
        self._acquisition_start = time.time()
        
        if self.enable_logging:
            logger.info("Entering AsyncResourceManager context")
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async exit and cleanup / Salida asíncrona y limpieza"""
        cleanup_errors = []
        
        if exc_val:
            logger.error("Exception in AsyncResourceManager context",
                        error_type=exc_type.__name__ if exc_type else None,
                        error_message=str(exc_val))
        
        # Cleanup nested managers
        for nested in reversed(self.nested_managers):
            try:
                await nested.__aexit__(None, None, None)
            except Exception as e:
                cleanup_errors.append(f"Nested manager: {str(e)}")
        
        # Cleanup resources
        for resource_name in reversed(list(self.resources.keys())):
            try:
                await self._cleanup_resource_async(resource_name)
            except Exception as e:
                cleanup_errors.append(f"{resource_name}: {str(e)}")
        
        # Close cleanup stack
        await self.cleanup_stack.__aexit__(exc_type, exc_val, exc_tb)
        
        if cleanup_errors:
            logger.error("Errors during async resource cleanup", errors=cleanup_errors)
        
        return False
    
    async def _cleanup_resource_async(self, resource_name: str):
        """Async resource cleanup / Limpieza asíncrona de recursos"""
        resource = self.resources.get(resource_name)
        if not resource:
            return
        
        if hasattr(resource, 'aclose'):
            await resource.aclose()
        elif hasattr(resource, 'close'):
            if asyncio.iscoroutinefunction(resource.close):
                await resource.close()
            else:
                resource.close()
        
        del self.resources[resource_name]