"""
Configuration module for the StreamScale system.
Módulo de configuración para el sistema StreamScale.
"""
import os
from typing import Optional
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

# Cargar variables de entorno / Load environment variables
load_dotenv()


class DatabaseConfig(BaseSettings):
    """Database configuration settings"""
    host: str = Field(default="localhost", env="POSTGRES_HOST")
    port: int = Field(default=5432, env="POSTGRES_PORT")
    database: str = Field(default="streamscale", env="POSTGRES_DB")
    user: str = Field(default="streamscale_user", env="POSTGRES_USER")
    password: str = Field(default="streamscale_password", env="POSTGRES_PASSWORD")
    pool_size: int = Field(default=20, env="POSTGRES_POOL_SIZE")
    max_overflow: int = Field(default=10, env="POSTGRES_MAX_OVERFLOW")
    pool_recycle: int = Field(default=3600, env="POSTGRES_POOL_RECYCLE")
    
    @property
    def url(self) -> str:
        """Get database connection URL"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    class Config:
        env_prefix = "POSTGRES_"


class RedisConfig(BaseSettings):
    """Redis configuration settings"""
    host: str = Field(default="localhost", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    db: int = Field(default=0, env="REDIS_DB")
    password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    pool_size: int = Field(default=50, env="REDIS_POOL_SIZE")
    cache_ttl: int = Field(default=3600, env="REDIS_CACHE_TTL")
    aggregation_ttl: int = Field(default=7200, env="REDIS_AGGREGATION_TTL")
    
    @property
    def url(self) -> str:
        """Get Redis connection URL"""
        return f"redis://{self.host}:{self.port}/{self.db}"
    
    class Config:
        env_prefix = "REDIS_"


class RabbitMQConfig(BaseSettings):
    """RabbitMQ configuration settings"""
    host: str = Field(default="localhost", env="RABBITMQ_HOST")
    port: int = Field(default=5672, env="RABBITMQ_PORT")
    user: str = Field(default="guest", env="RABBITMQ_USER")
    password: str = Field(default="guest", env="RABBITMQ_PASSWORD")
    vhost: str = Field(default="/", env="RABBITMQ_VHOST")
    
    @property
    def url(self) -> str:
        """Get RabbitMQ connection URL"""
        return f"amqp://{self.user}:{self.password}@{self.host}:{self.port}{self.vhost}"
    
    class Config:
        env_prefix = "RABBITMQ_"


class APIConfig(BaseSettings):
    """API configuration settings"""
    host: str = Field(default="0.0.0.0", env="API_HOST")
    port: int = Field(default=8000, env="API_PORT")
    workers: int = Field(default=4, env="API_WORKERS")
    
    class Config:
        env_prefix = "API_"


class SchedulerConfig(BaseSettings):
    """Scheduler configuration settings"""
    workers: int = Field(default=4, env="SCHEDULER_WORKERS")
    max_tasks: int = Field(default=1000, env="SCHEDULER_MAX_TASKS")
    task_timeout: int = Field(default=300, env="SCHEDULER_TASK_TIMEOUT")
    max_queue_size: int = Field(default=10000, env="SCHEDULER_MAX_QUEUE_SIZE")
    retry_attempts: int = Field(default=3, env="SCHEDULER_RETRY_ATTEMPTS")
    retry_delay: int = Field(default=1, env="SCHEDULER_RETRY_DELAY")
    
    class Config:
        env_prefix = "SCHEDULER_"


class Settings(BaseSettings):
    """Main application settings"""
    database: DatabaseConfig = DatabaseConfig()
    redis: RedisConfig = RedisConfig()
    rabbitmq: RabbitMQConfig = RabbitMQConfig()
    api: APIConfig = APIConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    
    # Logging configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(default="json", env="LOG_FORMAT")
    
    # Processing configuration
    batch_size: int = Field(default=100, env="BATCH_SIZE")
    window_size: int = Field(default=60, env="WINDOW_SIZE")
    max_queue_size: int = Field(default=5000, env="MAX_QUEUE_SIZE")
    
    # Performance settings
    enable_extreme_optimizations: bool = Field(default=True, env="ENABLE_EXTREME_OPTIMIZATIONS")
    memory_pool_size: int = Field(default=1048576, env="MEMORY_POOL_SIZE")
    
    # Testing
    testing: bool = Field(default=False, env="TESTING")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton instance / Instancia singleton
settings = Settings()


class Config:
    """Simple config access for backward compatibility"""
    # Database
    POSTGRES_HOST = settings.database.host
    POSTGRES_PORT = settings.database.port
    POSTGRES_DB = settings.database.database
    POSTGRES_USER = settings.database.user
    POSTGRES_PASSWORD = settings.database.password
    DB_POOL_SIZE = settings.database.pool_size
    DB_MAX_OVERFLOW = settings.database.max_overflow
    DB_POOL_RECYCLE = settings.database.pool_recycle
    
    # Redis
    REDIS_HOST = settings.redis.host
    REDIS_PORT = settings.redis.port
    REDIS_DB = settings.redis.db
    REDIS_PASSWORD = settings.redis.password
    REDIS_POOL_SIZE = settings.redis.pool_size
    REDIS_CACHE_TTL = settings.redis.cache_ttl
    REDIS_AGGREGATION_TTL = settings.redis.aggregation_ttl
    
    # RabbitMQ
    RABBITMQ_HOST = settings.rabbitmq.host
    RABBITMQ_PORT = settings.rabbitmq.port
    RABBITMQ_USER = settings.rabbitmq.user
    RABBITMQ_PASSWORD = settings.rabbitmq.password
    RABBITMQ_VHOST = settings.rabbitmq.vhost
    
    # Processing
    BATCH_SIZE = settings.batch_size
    WINDOW_SIZE = settings.window_size
    MAX_QUEUE_SIZE = settings.max_queue_size
    
    # Performance
    ENABLE_EXTREME_OPTIMIZATIONS = settings.enable_extreme_optimizations
    MEMORY_POOL_SIZE = settings.memory_pool_size
    
    # Testing
    TESTING = settings.testing
    
    @classmethod
    def get_database_url(cls) -> str:
        return settings.database.url
    
    @classmethod
    def get_async_database_url(cls) -> str:
        return settings.database.url.replace("postgresql://", "postgresql+asyncpg://")
    
    @classmethod
    def get_redis_url(cls) -> str:
        return settings.redis.url