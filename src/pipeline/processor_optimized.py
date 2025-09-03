"""
Optimized memory-efficient data processor using generators.
Procesador de datos optimizado y eficiente en memoria usando generadores.
"""
import json
import hashlib
from typing import Generator, Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, deque
from functools import lru_cache
from dataclasses import dataclass, field
import asyncio

# Disable logging for performance
class NullLogger:
    def info(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def debug(self, *args, **kwargs): pass

logger = NullLogger()


@dataclass
class FastDataEvent:
    """
    Optimized data event with minimal overhead.
    Evento de datos optimizado con mínimo overhead.
    """
    event_id: str
    timestamp: datetime
    source: str
    event_type: str
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class FastAggregatedData:
    """
    Optimized aggregated data model.
    Modelo de datos agregados optimizado.
    """
    aggregation_id: str
    window_start: datetime
    window_end: datetime
    source: str
    event_type: str
    count: int
    metrics: Dict[str, float]
    metadata: Optional[Dict[str, Any]] = None


class ObjectPool:
    """
    Object pool for reusing event objects.
    Pool de objetos para reusar objetos de eventos.
    """
    def __init__(self, max_size: int = 1000):
        self.pool = deque(maxlen=max_size)
        self.max_size = max_size
    
    def acquire(self, **kwargs) -> FastDataEvent:
        """Get object from pool or create new / Obtiene objeto del pool o crea nuevo"""
        if self.pool:
            obj = self.pool.popleft()
            # Update object attributes
            for key, value in kwargs.items():
                setattr(obj, key, value)
            return obj
        return FastDataEvent(**kwargs)
    
    def release(self, obj: FastDataEvent):
        """Return object to pool / Devuelve objeto al pool"""
        if len(self.pool) < self.max_size:
            self.pool.append(obj)


class OptimizedDataProcessor:
    """
    Highly optimized data processor with caching and object pooling.
    Procesador de datos altamente optimizado con caché y pooling de objetos.
    """
    
    def __init__(self, window_size_seconds: int = 60, batch_size: int = 100):
        """
        Initialize the optimized processor.
        """
        self.window_size_seconds = window_size_seconds
        self.batch_size = batch_size
        self.object_pool = ObjectPool(max_size=batch_size * 2)
        self.transform_cache = {}  # Simple cache for transformations
        self._aggregation_cache = {}  # Cache for aggregations
        
    @lru_cache(maxsize=10000)
    def _cached_transform(self, value: str) -> str:
        """
        Cached string transformation.
        Transformación de cadena con caché.
        """
        return value.upper() if isinstance(value, str) else str(value)
    
    def transform_event_fast(self, raw_data: Dict[str, Any]) -> Optional[FastDataEvent]:
        """
        Fast event transformation without validation overhead.
        Transformación rápida de eventos sin overhead de validación.
        """
        try:
            # Direct field access without validation
            event_id = raw_data.get('event_id')
            if not event_id:
                return None
            
            # Use object pool for efficiency
            event = self.object_pool.acquire(
                event_id=event_id,
                timestamp=datetime.utcnow(),
                source=raw_data.get('source', 'unknown'),
                event_type=raw_data.get('event_type', 'unknown'),
                data=self._fast_transform_data(raw_data.get('data', {})),
                metadata=raw_data.get('metadata')
            )
            
            return event
            
        except Exception:
            return None
    
    def _fast_transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fast data transformation with caching.
        Transformación rápida de datos con caché.
        """
        # Check cache first
        data_key = str(sorted(data.items()))
        if data_key in self.transform_cache:
            return self.transform_cache[data_key]
        
        transformed = {}
        for key, value in data.items():
            # Fast key normalization
            normalized_key = key.lower().replace(' ', '_')
            
            # Fast type conversion without try-except
            if isinstance(value, str) and value.isdigit():
                transformed[normalized_key] = int(value)
            elif isinstance(value, str) and '.' in value:
                try:
                    transformed[normalized_key] = float(value)
                except:
                    transformed[normalized_key] = value
            else:
                transformed[normalized_key] = value
        
        # Cache result (limit cache size)
        if len(self.transform_cache) < 1000:
            self.transform_cache[data_key] = transformed
        
        return transformed
    
    def aggregate_events_fast(self, 
                             events: Generator[FastDataEvent, None, None]) -> Generator[FastAggregatedData, None, None]:
        """
        Fast event aggregation using optimized data structures.
        Agregación rápida de eventos usando estructuras de datos optimizadas.
        """
        current_window_start = None
        
        # Use defaultdict with lambda for faster access
        window_data = defaultdict(lambda: {
            'count': 0,
            'metrics': defaultdict(list),
            'events': deque(maxlen=100)  # Limit stored events
        })
        
        for event in events:
            if event is None:
                continue
                
            # Initialize window if needed
            if current_window_start is None:
                current_window_start = event.timestamp
            
            # Fast window check
            window_end = current_window_start + timedelta(seconds=self.window_size_seconds)
            
            if event.timestamp >= window_end:
                # Yield aggregations for current window
                yield from self._create_fast_aggregations(window_data, current_window_start, window_end)
                
                # Reset for new window
                current_window_start = event.timestamp
                window_data.clear()
            
            # Fast aggregation
            key = (event.source, event.event_type)
            window_data[key]['count'] += 1
            window_data[key]['events'].append(event.event_id)
            
            # Aggregate numeric metrics efficiently
            for field, value in event.data.items():
                if isinstance(value, (int, float)):
                    window_data[key]['metrics'][field].append(value)
        
        # Yield final window
        if window_data and current_window_start:
            window_end = current_window_start + timedelta(seconds=self.window_size_seconds)
            yield from self._create_fast_aggregations(window_data, current_window_start, window_end)
    
    def _create_fast_aggregations(self, 
                                window_data: Dict[Tuple[str, str], Dict],
                                window_start: datetime,
                                window_end: datetime) -> Generator[FastAggregatedData, None, None]:
        """
        Create aggregations with optimized calculations.
        Crea agregaciones con cálculos optimizados.
        """
        for (source, event_type), data in window_data.items():
            # Fast metrics calculation
            metrics = {}
            for metric_name, values in data['metrics'].items():
                if values:
                    # Use built-in functions for speed
                    metrics[f"{metric_name}_min"] = min(values)
                    metrics[f"{metric_name}_max"] = max(values)
                    metrics[f"{metric_name}_avg"] = sum(values) / len(values)
                    metrics[f"{metric_name}_sum"] = sum(values)
            
            # Fast aggregation ID generation
            aggregation_id = f"{source}_{event_type}_{id(window_start)}"[:16]
            
            yield FastAggregatedData(
                aggregation_id=aggregation_id,
                window_start=window_start,
                window_end=window_end,
                source=source,
                event_type=event_type,
                count=data['count'],
                metrics=metrics,
                metadata={'event_ids': list(data['events'])}
            )
    
    def process_stream_batch(self, 
                           raw_events: Generator[Dict[str, Any], None, None],
                           batch_size: int = 1000) -> Generator[Tuple[List[FastDataEvent], List[FastAggregatedData]], None, None]:
        """
        Process stream in batches for better performance.
        Procesa stream en lotes para mejor rendimiento.
        """
        event_batch = []
        
        for raw_event in raw_events:
            event = self.transform_event_fast(raw_event)
            if event:
                event_batch.append(event)
                
                if len(event_batch) >= batch_size:
                    # Process batch
                    aggregations = list(self.aggregate_events_fast(iter(event_batch)))
                    yield (event_batch[:], aggregations)
                    event_batch.clear()
        
        # Process remaining
        if event_batch:
            aggregations = list(self.aggregate_events_fast(iter(event_batch)))
            yield (event_batch, aggregations)


class VectorizedProcessor:
    """
    Vectorized processor using numpy for numerical operations.
    Procesador vectorizado usando numpy para operaciones numéricas.
    """
    
    def __init__(self):
        try:
            import numpy as np
            self.np = np
            self.vectorized = True
        except ImportError:
            self.vectorized = False
    
    def process_numeric_batch(self, values: List[float]) -> Dict[str, float]:
        """
        Process numeric values in batch using vectorized operations.
        Procesa valores numéricos en lote usando operaciones vectorizadas.
        """
        if not self.vectorized or not values:
            # Fallback to regular processing
            return {
                'min': min(values) if values else 0,
                'max': max(values) if values else 0,
                'avg': sum(values) / len(values) if values else 0,
                'sum': sum(values) if values else 0
            }
        
        # Vectorized operations
        arr = self.np.array(values)
        return {
            'min': float(self.np.min(arr)),
            'max': float(self.np.max(arr)),
            'avg': float(self.np.mean(arr)),
            'sum': float(self.np.sum(arr)),
            'std': float(self.np.std(arr)),
            'median': float(self.np.median(arr))
        }


class AsyncOptimizedProcessor(OptimizedDataProcessor):
    """
    Asynchronous optimized processor for even better I/O handling.
    Procesador asíncrono optimizado para mejor manejo de I/O.
    """
    
    async def process_stream_async_fast(self,
                                       raw_events: Generator[Dict[str, Any], None, None],
                                       max_concurrent: int = 10) -> Generator[Tuple[FastDataEvent, FastAggregatedData], None, None]:
        """
        Process stream asynchronously with concurrency limit.
        Procesa stream asincrónicamente con límite de concurrencia.
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_event(raw_event):
            async with semaphore:
                event = self.transform_event_fast(raw_event)
                # Simulate async I/O
                await asyncio.sleep(0)
                return event
        
        # Process events concurrently
        tasks = []
        for raw_event in raw_events:
            task = asyncio.create_task(process_event(raw_event))
            tasks.append(task)
            
            if len(tasks) >= self.batch_size:
                events = await asyncio.gather(*tasks)
                tasks.clear()
                
                # Filter None values
                valid_events = [e for e in events if e is not None]
                
                # Generate aggregations
                for aggregation in self.aggregate_events_fast(iter(valid_events)):
                    for event in valid_events:
                        yield (event, aggregation)
        
        # Process remaining tasks
        if tasks:
            events = await asyncio.gather(*tasks)
            valid_events = [e for e in events if e is not None]
            
            for aggregation in self.aggregate_events_fast(iter(valid_events)):
                for event in valid_events:
                    yield (event, aggregation)