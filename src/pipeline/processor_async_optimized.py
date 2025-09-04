"""
Optimized asynchronous data processor with true async benefits.
Procesador de datos asíncrono optimizado con beneficios async reales.
"""
import asyncio
import json
from typing import AsyncGenerator, Dict, Any, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from src.pipeline.models import DataEvent, AggregatedData
from src.shared.logging import get_logger

logger = get_logger(__name__)


class OptimizedAsyncDataProcessor:
    """
    Truly asynchronous data processor optimized for I/O operations.
    Procesador de datos verdaderamente asíncrono optimizado para operaciones I/O.
    """
    
    def __init__(self, window_size_seconds: int = 60, batch_size: int = 100):
        """
        Initialize the async processor.
        
        Args:
            window_size_seconds: Size of aggregation window in seconds
            batch_size: Batch size for processing
        """
        self.window_size_seconds = window_size_seconds
        self.batch_size = batch_size
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.semaphore = asyncio.Semaphore(100)  # Limit concurrent operations
        
    async def transform_event_async(self, raw_data: Dict[str, Any]) -> Optional[DataEvent]:
        """
        Asynchronously transform raw data into DataEvent.
        Only uses async for actual I/O operations.
        """
        try:
            # CPU-bound work stays synchronous (no benefit from async)
            event = DataEvent(**raw_data)
            event.data = self._apply_transformations_sync(event.data)
            
            if event.metadata is None:
                event.metadata = {}
            event.metadata['processed_at'] = datetime.utcnow().isoformat()
            event.metadata['processor_version'] = '2.0.0'
            
            return event
            
        except Exception as e:
            logger.error(f"Failed to transform event: {str(e)}")
            return None
    
    def _apply_transformations_sync(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronous transformation (CPU-bound, no async benefit).
        """
        transformed = {}
        for key, value in data.items():
            normalized_key = key.lower().replace(' ', '_')
            
            if isinstance(value, str):
                try:
                    if '.' in value:
                        transformed[normalized_key] = float(value)
                    else:
                        transformed[normalized_key] = int(value)
                except ValueError:
                    transformed[normalized_key] = value
            else:
                transformed[normalized_key] = value
        
        return transformed
    
    async def process_batch_async(self, events: List[Dict[str, Any]]) -> List[DataEvent]:
        """
        Process a batch of events concurrently.
        Procesa un lote de eventos concurrentemente.
        """
        tasks = []
        async with self.semaphore:
            for event_data in events:
                task = asyncio.create_task(self.transform_event_async(event_data))
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
        # Filter out None and exceptions
        processed = []
        for result in results:
            if isinstance(result, DataEvent):
                processed.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Error processing event: {result}")
        
        return processed
    
    async def process_stream_async(self, 
                                 raw_events: AsyncGenerator[Dict[str, Any], None]) -> AsyncGenerator[DataEvent, None]:
        """
        Truly async stream processing with concurrent batch operations.
        Procesamiento de stream verdaderamente async con operaciones batch concurrentes.
        """
        batch = []
        
        async for raw_event in raw_events:
            batch.append(raw_event)
            
            if len(batch) >= self.batch_size:
                # Process batch concurrently
                processed = await self.process_batch_async(batch)
                for event in processed:
                    yield event
                batch.clear()
        
        # Process remaining batch
        if batch:
            processed = await self.process_batch_async(batch)
            for event in processed:
                yield event
    
    async def aggregate_events_async(self, 
                                    events: AsyncGenerator[DataEvent, None]) -> AsyncGenerator[AggregatedData, None]:
        """
        Asynchronously aggregate events using sliding windows.
        Agrega eventos asincrónicamente usando ventanas deslizantes.
        """
        windows = defaultdict(lambda: {
            'count': 0,
            'metrics': defaultdict(list),
            'start_time': datetime.utcnow()
        })
        
        async for event in events:
            # Create window key
            window_key = f"{event.source}_{event.event_type}"
            window = windows[window_key]
            
            # Update window
            window['count'] += 1
            
            # Collect metrics
            for key, value in event.data.items():
                if isinstance(value, (int, float)):
                    window['metrics'][key].append(value)
            
            # Check if window should be closed
            window_age = (datetime.utcnow() - window['start_time']).total_seconds()
            
            if window_age >= self.window_size_seconds or window['count'] >= self.batch_size:
                # Create aggregation
                metrics = {}
                for metric_name, values in window['metrics'].items():
                    if values:
                        metrics[f"{metric_name}_avg"] = sum(values) / len(values)
                        metrics[f"{metric_name}_sum"] = sum(values)
                        metrics[f"{metric_name}_min"] = min(values)
                        metrics[f"{metric_name}_max"] = max(values)
                
                aggregation = AggregatedData(
                    aggregation_id=f"agg_{window_key}_{datetime.utcnow().timestamp()}",
                    window_start=window['start_time'],
                    window_end=datetime.utcnow(),
                    source=event.source,
                    event_type=event.event_type,
                    count=window['count'],
                    metrics=metrics
                )
                
                yield aggregation
                
                # Reset window
                del windows[window_key]
    
    async def process_with_io(self, 
                            raw_events: AsyncGenerator[Dict[str, Any], None],
                            db_pool: Optional[Any] = None) -> AsyncGenerator[Tuple[DataEvent, bool], None]:
        """
        Process events with actual I/O operations (database writes).
        This shows real async benefits.
        """
        async for event in self.process_stream_async(raw_events):
            # Simulate or perform actual I/O
            if db_pool:
                try:
                    async with db_pool.acquire() as conn:
                        # Async database write
                        await conn.execute("""
                            INSERT INTO events (event_id, source, event_type, data, timestamp)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (event_id) DO NOTHING
                        """, event.event_id, event.source, event.event_type, 
                            json.dumps(event.data), event.timestamp)
                    
                    yield (event, True)
                except Exception as e:
                    logger.error(f"Database write failed: {e}")
                    yield (event, False)
            else:
                # Simulate I/O delay
                await asyncio.sleep(0.001)  # 1ms simulated I/O
                yield (event, True)
    
    async def cleanup(self):
        """
        Cleanup resources.
        Limpia recursos.
        """
        self.executor.shutdown(wait=True)


class AsyncBatchProcessor:
    """
    Optimized batch processor using async for parallel operations.
    Procesador batch optimizado usando async para operaciones paralelas.
    """
    
    def __init__(self, concurrency: int = 10):
        """Initialize with concurrency limit."""
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
    
    async def process_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process single item with simulated I/O."""
        async with self.semaphore:
            # Simulate I/O operation
            await asyncio.sleep(0.01)  # 10ms I/O
            
            # Transform
            result = {
                'id': item.get('event_id', 'unknown'),
                'processed': True,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return result
    
    async def process_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process batch with true concurrency.
        Procesa lote con verdadera concurrencia.
        """
        tasks = [self.process_item(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful results
        return [r for r in results if isinstance(r, dict)]


# Helper function to create async generator from sync data
async def async_generator_from_list(items: List[Any]) -> AsyncGenerator[Any, None]:
    """Convert list to async generator."""
    for item in items:
        yield item
        # Small yield to allow other coroutines
        if len(items) > 100:
            await asyncio.sleep(0)