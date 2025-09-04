"""
Shared connection utilities for database and cache services.
"""
from typing import Optional, Dict, Any
import redis
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import pika
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.pool import NullPool

from src.shared.config import Config
from src.shared.logging import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    """Centralized connection management for all services"""
    
    _redis_client: Optional[redis.Redis] = None
    _db_pool: Optional[SimpleConnectionPool] = None
    _rabbitmq_connection: Optional[pika.BlockingConnection] = None
    _async_engine: Optional[AsyncEngine] = None
    
    @classmethod
    def get_redis_client(cls, decode_responses: bool = True) -> redis.Redis:
        """Get or create Redis client with connection pooling"""
        if cls._redis_client is None:
            pool = redis.ConnectionPool(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=Config.REDIS_DB,
                password=Config.REDIS_PASSWORD,
                max_connections=Config.REDIS_POOL_SIZE,
                socket_keepalive=True,
                socket_keepalive_options={
                    1: 1,  # TCP_KEEPIDLE
                    2: 1,  # TCP_KEEPINTVL  
                    3: 5,  # TCP_KEEPCNT
                },
                decode_responses=decode_responses
            )
            cls._redis_client = redis.Redis(connection_pool=pool)
            logger.info("Redis connection pool initialized")
        return cls._redis_client
    
    @classmethod
    def get_db_connection(cls):
        """Get database connection from pool"""
        if cls._db_pool is None:
            cls._db_pool = SimpleConnectionPool(
                1,
                Config.DB_POOL_SIZE,
                host=Config.POSTGRES_HOST,
                port=Config.POSTGRES_PORT,
                database=Config.POSTGRES_DB,
                user=Config.POSTGRES_USER,
                password=Config.POSTGRES_PASSWORD
            )
            logger.info("Database connection pool initialized")
        return cls._db_pool.getconn()
    
    @classmethod
    def return_db_connection(cls, conn):
        """Return database connection to pool"""
        if cls._db_pool:
            cls._db_pool.putconn(conn)
    
    @classmethod
    def get_rabbitmq_connection(cls) -> pika.BlockingConnection:
        """Get or create RabbitMQ connection"""
        if cls._rabbitmq_connection is None or cls._rabbitmq_connection.is_closed:
            params = pika.ConnectionParameters(
                host=Config.RABBITMQ_HOST,
                port=Config.RABBITMQ_PORT,
                virtual_host=Config.RABBITMQ_VHOST,
                credentials=pika.PlainCredentials(
                    Config.RABBITMQ_USER,
                    Config.RABBITMQ_PASSWORD
                ),
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=3,
                retry_delay=2
            )
            cls._rabbitmq_connection = pika.BlockingConnection(params)
            logger.info("RabbitMQ connection established")
        return cls._rabbitmq_connection
    
    @classmethod
    def get_async_db_engine(cls) -> AsyncEngine:
        """Get async database engine for SQLAlchemy"""
        if cls._async_engine is None:
            cls._async_engine = create_async_engine(
                Config.get_async_database_url(),
                echo=False,
                pool_size=Config.DB_POOL_SIZE,
                max_overflow=Config.DB_MAX_OVERFLOW,
                pool_recycle=Config.DB_POOL_RECYCLE,
                pool_pre_ping=True,
                poolclass=NullPool if Config.TESTING else None
            )
            logger.info("Async database engine initialized")
        return cls._async_engine
    
    @classmethod
    async def close_all(cls):
        """Close all connections gracefully"""
        if cls._redis_client:
            cls._redis_client.close()
            cls._redis_client = None
            
        if cls._db_pool:
            cls._db_pool.closeall()
            cls._db_pool = None
            
        if cls._rabbitmq_connection and not cls._rabbitmq_connection.is_closed:
            cls._rabbitmq_connection.close()
            cls._rabbitmq_connection = None
            
        if cls._async_engine:
            await cls._async_engine.dispose()
            cls._async_engine = None
            
        logger.info("All connections closed")