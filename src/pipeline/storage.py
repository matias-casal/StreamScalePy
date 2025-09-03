"""
Storage module for pipeline data.
Módulo de almacenamiento para datos del pipeline.
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, select, delete, func, and_
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import redis.asyncio as redis
import json

from src.pipeline.models import Base, ProcessedEventDB, AggregationDB, DataEvent, AggregatedData
from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


class DataStorage:
    """
    Handles data storage operations for the pipeline.
    Maneja operaciones de almacenamiento de datos para el pipeline.
    """
    
    def __init__(self):
        """Initialize storage components"""
        self.engine = None
        self.async_session_maker = None
        self.redis_client = None
        self._initialized = False
    
    async def initialize(self):
        """
        Initialize database and cache connections.
        Inicializa conexiones a base de datos y caché.
        """
        try:
            # Create async engine for PostgreSQL
            db_url = settings.database.url.replace('postgresql://', 'postgresql+asyncpg://')
            self.engine = create_async_engine(
                db_url,
                echo=False,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Create session maker
            self.async_session_maker = sessionmaker(
                self.engine, 
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Create tables
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            # Initialize Redis client
            self.redis_client = await redis.from_url(
                settings.redis.url,
                encoding="utf-8",
                decode_responses=True
            )
            
            # Test connections
            await self.redis_client.ping()
            
            self._initialized = True
            logger.info("Storage initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize storage", error=str(e))
            raise
    
    async def close(self):
        """
        Close all connections.
        Cierra todas las conexiones.
        """
        if self.engine:
            await self.engine.dispose()
        
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("Storage connections closed")
    
    async def is_connected(self) -> bool:
        """Check if storage is connected / Verifica si el almacenamiento está conectado"""
        if not self._initialized:
            return False
        
        try:
            # Test Redis
            await self.redis_client.ping()
            
            # Test PostgreSQL
            async with self.async_session_maker() as session:
                await session.execute(select(1))
            
            return True
        except Exception:
            return False
    
    @asynccontextmanager
    async def get_session(self):
        """
        Get database session context manager.
        Obtiene el administrador de contexto de sesión de base de datos.
        """
        async with self.async_session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    async def store_event(self, event: DataEvent) -> ProcessedEventDB:
        """
        Store a processed event in the database.
        Almacena un evento procesado en la base de datos.
        """
        async with self.get_session() as session:
            try:
                # Check for duplicate
                existing = await session.execute(
                    select(ProcessedEventDB).where(
                        ProcessedEventDB.event_id == event.event_id
                    )
                )
                
                if existing.scalar_one_or_none():
                    logger.warning("Duplicate event", event_id=event.event_id)
                    return existing.scalar_one()
                
                # Create database record
                db_event = ProcessedEventDB(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    source=event.source,
                    event_type=event.event_type,
                    data=event.data,
                    processed_at=datetime.utcnow()
                )
                
                session.add(db_event)
                await session.flush()
                
                # Cache in Redis with expiry
                cache_key = f"event:{event.event_id}"
                await self.redis_client.setex(
                    cache_key,
                    3600,  # 1 hour TTL
                    json.dumps(event.dict())
                )
                
                logger.debug("Event stored", event_id=event.event_id)
                return db_event
                
            except Exception as e:
                logger.error("Failed to store event", 
                           event_id=event.event_id,
                           error=str(e))
                raise
    
    async def store_aggregation(self, aggregation: AggregatedData) -> AggregationDB:
        """
        Store an aggregation in the database.
        Almacena una agregación en la base de datos.
        """
        async with self.get_session() as session:
            try:
                # Check for duplicate
                existing = await session.execute(
                    select(AggregationDB).where(
                        AggregationDB.aggregation_id == aggregation.aggregation_id
                    )
                )
                
                if existing.scalar_one_or_none():
                    logger.warning("Duplicate aggregation", 
                                 aggregation_id=aggregation.aggregation_id)
                    return existing.scalar_one()
                
                # Create database record
                db_aggregation = AggregationDB(
                    aggregation_id=aggregation.aggregation_id,
                    window_start=aggregation.window_start,
                    window_end=aggregation.window_end,
                    source=aggregation.source,
                    event_type=aggregation.event_type,
                    event_count=aggregation.count,
                    metrics=aggregation.metrics
                )
                
                session.add(db_aggregation)
                await session.flush()
                
                # Cache aggregation
                cache_key = f"aggregation:{aggregation.aggregation_id}"
                await self.redis_client.setex(
                    cache_key,
                    7200,  # 2 hour TTL
                    json.dumps(aggregation.dict(), default=str)
                )
                
                # Update statistics in Redis
                stats_key = f"stats:{aggregation.source}:{aggregation.event_type}"
                await self.redis_client.hincrby(stats_key, "total_events", aggregation.count)
                await self.redis_client.hincrby(stats_key, "total_aggregations", 1)
                
                logger.debug("Aggregation stored", 
                           aggregation_id=aggregation.aggregation_id)
                return db_aggregation
                
            except Exception as e:
                logger.error("Failed to store aggregation",
                           aggregation_id=aggregation.aggregation_id,
                           error=str(e))
                raise
    
    async def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an event by ID.
        Recupera un evento por ID.
        """
        # Check cache first
        cache_key = f"event:{event_id}"
        cached = await self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        
        # Query database
        async with self.get_session() as session:
            result = await session.execute(
                select(ProcessedEventDB).where(
                    ProcessedEventDB.event_id == event_id
                )
            )
            event = result.scalar_one_or_none()
            
            if event:
                return event.to_dict()
            
            return None
    
    async def get_aggregation(self, aggregation_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an aggregation by ID.
        Recupera una agregación por ID.
        """
        # Check cache first
        cache_key = f"aggregation:{aggregation_id}"
        cached = await self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        
        # Query database
        async with self.get_session() as session:
            result = await session.execute(
                select(AggregationDB).where(
                    AggregationDB.aggregation_id == aggregation_id
                )
            )
            aggregation = result.scalar_one_or_none()
            
            if aggregation:
                return aggregation.to_dict()
            
            return None
    
    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get storage statistics.
        Obtiene estadísticas de almacenamiento.
        """
        async with self.get_session() as session:
            # Count events
            event_count = await session.execute(
                select(func.count()).select_from(ProcessedEventDB)
            )
            
            # Count aggregations  
            aggregation_count = await session.execute(
                select(func.count()).select_from(AggregationDB)
            )
            
            # Get recent events
            recent_events = await session.execute(
                select(ProcessedEventDB).order_by(
                    ProcessedEventDB.processed_at.desc()
                ).limit(10)
            )
            
            return {
                "total_events": event_count.scalar(),
                "total_aggregations": aggregation_count.scalar(),
                "recent_events": [e.to_dict() for e in recent_events.scalars()],
                "cache_keys": await self.redis_client.dbsize()
            }
    
    async def cleanup_old_data(self, days_old: int = 30) -> int:
        """
        Clean up old data from storage.
        Limpia datos antiguos del almacenamiento.
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        deleted_count = 0
        
        async with self.get_session() as session:
            # Delete old events
            result = await session.execute(
                delete(ProcessedEventDB).where(
                    ProcessedEventDB.processed_at < cutoff_date
                )
            )
            deleted_count += result.rowcount
            
            # Delete old aggregations
            result = await session.execute(
                delete(AggregationDB).where(
                    AggregationDB.created_at < cutoff_date
                )
            )
            deleted_count += result.rowcount
        
        logger.info("Cleaned up old data", 
                   deleted_records=deleted_count,
                   cutoff_date=cutoff_date.isoformat())
        
        return deleted_count