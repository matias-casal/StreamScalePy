"""
FastAPI webhook endpoint for data ingestion.
Endpoint FastAPI webhook para ingesta de datos.
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status
from fastapi.responses import JSONResponse
from typing import Dict, Any, List, Optional
import asyncio
import json
from datetime import datetime
from contextlib import asynccontextmanager

from src.pipeline.models import DataEvent, AggregatedData
from src.pipeline.processor import AsyncDataProcessor
from src.pipeline.storage import DataStorage
from src.pipeline.queue_manager import QueueManager
from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)

# Global instances
processor: Optional[AsyncDataProcessor] = None
storage: Optional[DataStorage] = None
queue_manager: Optional[QueueManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle.
    Gestiona el ciclo de vida de la aplicación.
    """
    global processor, storage, queue_manager
    
    logger.info("Initializing pipeline API")
    
    # Initialize components
    processor = AsyncDataProcessor(
        window_size_seconds=60,
        batch_size=100
    )
    storage = DataStorage()
    queue_manager = QueueManager()
    
    # Initialize connections
    await storage.initialize()
    await queue_manager.initialize()
    
    logger.info("Pipeline API initialized successfully")
    
    yield
    
    # Cleanup
    logger.info("Shutting down pipeline API")
    await storage.close()
    await queue_manager.close()


# Create FastAPI app
app = FastAPI(
    title="StreamScale Data Pipeline",
    description="Memory-efficient data processing pipeline with webhook endpoint",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
async def root():
    """Root endpoint / Endpoint raíz"""
    return {
        "service": "StreamScale Data Pipeline",
        "status": "operational",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    Endpoint de verificación de salud.
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "processor": processor is not None,
            "storage": storage is not None and await storage.is_connected(),
            "queue": queue_manager is not None and await queue_manager.is_connected()
        }
    }
    
    # Check if all components are healthy
    all_healthy = all(health_status["components"].values())
    
    if not all_healthy:
        logger.warning("Health check failed", components=health_status["components"])
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=health_status
        )
    
    return health_status


@app.post("/webhook/ingest")
async def ingest_data(
    data: Dict[str, Any],
    background_tasks: BackgroundTasks
):
    """
    Webhook endpoint for data ingestion.
    Endpoint webhook para ingesta de datos.
    
    This endpoint receives JSON data and processes it asynchronously.
    Este endpoint recibe datos JSON y los procesa asincrónicamente.
    """
    try:
        logger.info("Received webhook data", data_keys=list(data.keys()))
        
        # Validate basic structure
        if not data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Empty data received"
            )
        
        # Add to background processing
        background_tasks.add_task(
            process_single_event,
            data
        )
        
        return {
            "status": "accepted",
            "message": "Data queued for processing",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error("Failed to ingest data", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process data: {str(e)}"
        )


@app.post("/webhook/batch")
async def ingest_batch(
    events: List[Dict[str, Any]],
    background_tasks: BackgroundTasks
):
    """
    Batch webhook endpoint for multiple events.
    Endpoint webhook por lotes para múltiples eventos.
    """
    try:
        logger.info(f"Received batch of {len(events)} events")
        
        if not events:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Empty batch received"
            )
        
        # Process batch in background
        background_tasks.add_task(
            process_batch_events,
            events
        )
        
        return {
            "status": "accepted",
            "message": f"Batch of {len(events)} events queued for processing",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error("Failed to ingest batch", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process batch: {str(e)}"
        )


async def process_single_event(raw_data: Dict[str, Any]):
    """
    Process a single event asynchronously.
    Procesa un único evento asincrónicamente.
    """
    try:
        # Create generator from single event
        events_gen = (e for e in [raw_data])
        
        # Process through pipeline
        async for event, aggregation in processor.process_stream_async(events_gen):
            if event:
                # Store in database
                await storage.store_event(event)
                
                # Send to message queue
                await queue_manager.publish_event(event)
                
                logger.info("Event processed", event_id=event.event_id)
            
            if aggregation:
                # Store aggregation
                await storage.store_aggregation(aggregation)
                
                # Send aggregation to queue
                await queue_manager.publish_aggregation(aggregation)
                
                logger.info("Aggregation stored", aggregation_id=aggregation.aggregation_id)
                
    except Exception as e:
        logger.error("Failed to process event", error=str(e), raw_data=raw_data)


async def process_batch_events(events: List[Dict[str, Any]]):
    """
    Process a batch of events efficiently.
    Procesa un lote de eventos eficientemente.
    """
    try:
        # Create generator from batch
        events_gen = (e for e in events)
        
        # Process through pipeline with memory-efficient streaming
        event_count = 0
        aggregation_count = 0
        
        async for event, aggregation in processor.process_stream_async(events_gen):
            if event:
                await storage.store_event(event)
                await queue_manager.publish_event(event)
                event_count += 1
            
            if aggregation:
                await storage.store_aggregation(aggregation)
                await queue_manager.publish_aggregation(aggregation)
                aggregation_count += 1
        
        logger.info("Batch processed", 
                   events_processed=event_count,
                   aggregations_created=aggregation_count)
                   
    except Exception as e:
        logger.error("Failed to process batch", error=str(e))


@app.get("/stats")
async def get_statistics():
    """
    Get processing statistics.
    Obtiene estadísticas de procesamiento.
    """
    try:
        stats = await storage.get_statistics()
        queue_stats = await queue_manager.get_queue_stats()
        
        return {
            "storage": stats,
            "queues": queue_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error("Failed to get statistics", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve statistics"
        )


@app.delete("/data/cleanup")
async def cleanup_old_data(days_old: int = 30):
    """
    Clean up old data from storage.
    Limpia datos antiguos del almacenamiento.
    """
    try:
        deleted_count = await storage.cleanup_old_data(days_old)
        return {
            "status": "success",
            "deleted_records": deleted_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error("Failed to cleanup data", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cleanup data"
        )