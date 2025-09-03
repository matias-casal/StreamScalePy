"""
Memory-efficient data processor using generators.
Procesador de datos eficiente en memoria usando generadores.
"""
import json
import hashlib
from typing import Generator, Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
from src.pipeline.models import DataEvent, AggregatedData
from src.shared.logging import get_logger

logger = get_logger(__name__)


class DataProcessor:
    """
    Memory-efficient data processor that uses generators for streaming processing.
    Procesador de datos eficiente en memoria que usa generadores para procesamiento en streaming.
    """
    
    def __init__(self, window_size_seconds: int = 60, batch_size: int = 100):
        """
        Initialize the processor.
        
        Args:
            window_size_seconds: Size of aggregation window in seconds
            batch_size: Batch size for processing
        """
        self.window_size_seconds = window_size_seconds
        self.batch_size = batch_size
        self.aggregation_buffer = defaultdict(lambda: {
            'count': 0,
            'metrics': defaultdict(list),
            'events': []
        })
        
    def transform_event(self, raw_data: Dict[str, Any]) -> Generator[DataEvent, None, None]:
        """
        Transform raw data into DataEvent using generator for memory efficiency.
        Transforma datos crudos en DataEvent usando generador para eficiencia de memoria.
        
        Yields:
            Transformed DataEvent objects
        """
        try:
            # Validate and transform the event
            event = DataEvent(**raw_data)
            
            # Apply transformations
            event.data = self._apply_transformations(event.data)
            
            # Add processing metadata
            if event.metadata is None:
                event.metadata = {}
            event.metadata['processed_at'] = datetime.utcnow().isoformat()
            event.metadata['processor_version'] = '1.0.0'
            
            logger.info("Event transformed", 
                       event_id=event.event_id,
                       event_type=event.event_type)
            
            yield event
            
        except Exception as e:
            logger.error("Failed to transform event", 
                        error=str(e),
                        raw_data=raw_data)
            # Yield None to indicate error but continue processing
            yield None
    
    def _apply_transformations(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply data transformations.
        Aplica transformaciones de datos.
        """
        transformed = {}
        
        for key, value in data.items():
            # Normalize keys to lowercase
            normalized_key = key.lower().replace(' ', '_')
            
            # Type conversions and validations
            if isinstance(value, str):
                # Try to parse numeric strings
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
    
    def aggregate_events(self, 
                        events: Generator[DataEvent, None, None]) -> Generator[AggregatedData, None, None]:
        """
        Aggregate events using sliding windows with generators.
        Agrega eventos usando ventanas deslizantes con generadores.
        
        Yields:
            AggregatedData objects when windows complete
        """
        current_window_start = None
        window_data = defaultdict(lambda: {
            'count': 0,
            'metrics': defaultdict(list),
            'events': []
        })
        
        for event in events:
            if event is None:
                continue
                
            # Initialize window if needed
            if current_window_start is None:
                current_window_start = event.timestamp
            
            # Check if we need to close the current window
            window_end = current_window_start + timedelta(seconds=self.window_size_seconds)
            
            if event.timestamp >= window_end:
                # Yield aggregations for current window
                yield from self._create_aggregations(window_data, current_window_start, window_end)
                
                # Start new window
                current_window_start = event.timestamp
                window_data.clear()
            
            # Add event to current window
            key = (event.source, event.event_type)
            window_data[key]['count'] += 1
            window_data[key]['events'].append(event.event_id)
            
            # Aggregate numeric metrics
            for field, value in event.data.items():
                if isinstance(value, (int, float)):
                    window_data[key]['metrics'][field].append(value)
        
        # Yield final window if there's data
        if window_data and current_window_start:
            window_end = current_window_start + timedelta(seconds=self.window_size_seconds)
            yield from self._create_aggregations(window_data, current_window_start, window_end)
    
    def _create_aggregations(self, 
                           window_data: Dict[Tuple[str, str], Dict],
                           window_start: datetime,
                           window_end: datetime) -> Generator[AggregatedData, None, None]:
        """
        Create aggregation objects from window data.
        Crea objetos de agregación desde datos de ventana.
        """
        for (source, event_type), data in window_data.items():
            # Calculate metrics
            metrics = {}
            for metric_name, values in data['metrics'].items():
                if values:
                    metrics[f"{metric_name}_min"] = min(values)
                    metrics[f"{metric_name}_max"] = max(values)
                    metrics[f"{metric_name}_avg"] = sum(values) / len(values)
                    metrics[f"{metric_name}_sum"] = sum(values)
            
            # Generate aggregation ID
            aggregation_id = self._generate_aggregation_id(
                source, event_type, window_start, window_end
            )
            
            yield AggregatedData(
                aggregation_id=aggregation_id,
                window_start=window_start,
                window_end=window_end,
                source=source,
                event_type=event_type,
                count=data['count'],
                metrics=metrics,
                metadata={
                    'event_ids': data['events'][:100]  # Limit stored event IDs
                }
            )
            
            logger.info("Aggregation created",
                       aggregation_id=aggregation_id,
                       event_count=data['count'])
    
    def _generate_aggregation_id(self, 
                                source: str, 
                                event_type: str,
                                window_start: datetime,
                                window_end: datetime) -> str:
        """
        Generate unique aggregation ID.
        Genera ID único de agregación.
        """
        content = f"{source}:{event_type}:{window_start.isoformat()}:{window_end.isoformat()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def process_stream(self, 
                       raw_events: Generator[Dict[str, Any], None, None]) -> Generator[Tuple[Optional[DataEvent], Optional[AggregatedData]], None, None]:
        """
        Process a stream of raw events efficiently.
        Procesa un stream de eventos crudos eficientemente.
        
        This is the main entry point for processing that chains transformations and aggregations.
        Este es el punto de entrada principal para procesamiento que encadena transformaciones y agregaciones.
        
        Yields:
            Tuples of (transformed_event, aggregation) where aggregation may be None
        """
        # Buffer for batching
        event_buffer = []
        
        for raw_event in raw_events:
            # Transform the event
            for transformed_event in self.transform_event(raw_event):
                if transformed_event:
                    event_buffer.append(transformed_event)
                    
                    # Process batch when buffer is full
                    if len(event_buffer) >= self.batch_size:
                        # Create generator from buffer
                        events_gen = (e for e in event_buffer)
                        
                        # Aggregate and yield results
                        for aggregation in self.aggregate_events(events_gen):
                            # Yield both event and aggregation
                            for event in event_buffer:
                                yield (event, aggregation)
                        
                        # Clear buffer
                        event_buffer.clear()
        
        # Process remaining events in buffer
        if event_buffer:
            events_gen = (e for e in event_buffer)
            for aggregation in self.aggregate_events(events_gen):
                for event in event_buffer:
                    yield (event, aggregation)


class AsyncDataProcessor(DataProcessor):
    """
    Asynchronous version of the data processor for better I/O handling.
    Versión asíncrona del procesador de datos para mejor manejo de I/O.
    """
    
    async def process_stream_async(self,
                                  raw_events: Generator[Dict[str, Any], None, None]) -> Generator[Tuple[Optional[DataEvent], Optional[AggregatedData]], None, None]:
        """
        Asynchronously process a stream of events.
        Procesa asincrónicamente un stream de eventos.
        """
        # Use asyncio for better I/O handling
        loop = asyncio.get_event_loop()
        
        # Process in background
        for result in self.process_stream(raw_events):
            # Allow other coroutines to run
            await asyncio.sleep(0)
            yield result