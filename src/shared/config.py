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
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton instance / Instancia singleton
settings = Settings()