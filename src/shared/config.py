"""
Configuration module for the StreamScale system.
Módulo de configuración para el sistema StreamScale.
"""
import os
from typing import Optional
from dotenv import load_dotenv
from pydantic import Field, ConfigDict
from pydantic_settings import BaseSettings

# Cargar variables de entorno / Load environment variables
load_dotenv()


class DatabaseConfig(BaseSettings):
    """Database configuration settings"""
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="streamscale", validation_alias="POSTGRES_DB")
    user: str = Field(default="streamscale_user")
    password: str = Field(default="streamscale_password")
    pool_size: int = Field(default=20)
    max_overflow: int = Field(default=10)
    pool_recycle: int = Field(default=3600)
    
    @property
    def url(self) -> str:
        """Get database connection URL"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    model_config = ConfigDict(
        env_prefix="POSTGRES_",
        extra="allow"
    )


class RedisConfig(BaseSettings):
    """Redis configuration settings"""
    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    db: int = Field(default=0)
    password: Optional[str] = Field(default=None)
    pool_size: int = Field(default=50)
    cache_ttl: int = Field(default=3600)
    aggregation_ttl: int = Field(default=7200)
    
    @property
    def url(self) -> str:
        """Get Redis connection URL"""
        return f"redis://{self.host}:{self.port}/{self.db}"
    
    model_config = ConfigDict(
        env_prefix="REDIS_",
        extra="allow"
    )


class RabbitMQConfig(BaseSettings):
    """RabbitMQ configuration settings"""
    host: str = Field(default="localhost")
    port: int = Field(default=5672)
    user: str = Field(default="guest")
    password: str = Field(default="guest")
    vhost: str = Field(default="/")
    
    @property
    def url(self) -> str:
        """Get RabbitMQ connection URL"""
        return f"amqp://{self.user}:{self.password}@{self.host}:{self.port}{self.vhost}"
    
    model_config = ConfigDict(
        env_prefix="RABBITMQ_",
        extra="allow"
    )


class APIConfig(BaseSettings):
    """API configuration settings"""
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    workers: int = Field(default=4)
    
    model_config = ConfigDict(
        env_prefix="API_",
        extra="allow"
    )


class SchedulerConfig(BaseSettings):
    """Scheduler configuration settings"""
    workers: int = Field(default=4)
    max_tasks: int = Field(default=1000)
    task_timeout: int = Field(default=300)
    max_queue_size: int = Field(default=10000)
    retry_attempts: int = Field(default=3)
    retry_delay: int = Field(default=1)
    
    model_config = ConfigDict(
        env_prefix="SCHEDULER_",
        extra="allow"
    )


class Settings(BaseSettings):
    """Main application settings"""
    database: DatabaseConfig = DatabaseConfig()
    redis: RedisConfig = RedisConfig()
    rabbitmq: RabbitMQConfig = RabbitMQConfig()
    api: APIConfig = APIConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    
    # Logging configuration
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")
    
    # Processing configuration
    batch_size: int = Field(default=100)
    window_size: int = Field(default=60)
    max_queue_size: int = Field(default=5000)
    
    # Performance settings
    enable_extreme_optimizations: bool = Field(default=True)
    memory_pool_size: int = Field(default=1048576)
    
    # Testing
    testing: bool = Field(default=False)
    
    model_config = ConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="allow"
    )


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