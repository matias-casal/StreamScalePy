"""
Queue manager for RabbitMQ integration.
Gestor de colas para integración con RabbitMQ.
"""
import json
import asyncio
from typing import Dict, Any, Optional
import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode

from src.pipeline.models import DataEvent, AggregatedData
from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


class QueueManager:
    """
    Manages message queue operations with RabbitMQ.
    Gestiona operaciones de cola de mensajes con RabbitMQ.
    """
    
    def __init__(self):
        """Initialize queue manager"""
        self.connection = None
        self.channel = None
        self.events_exchange = None
        self.aggregations_exchange = None
        self._initialized = False
    
    async def initialize(self):
        """
        Initialize RabbitMQ connection and setup queues.
        Inicializa conexión RabbitMQ y configura colas.
        """
        try:
            # Connect to RabbitMQ
            self.connection = await aio_pika.connect_robust(
                settings.rabbitmq.url,
                loop=asyncio.get_event_loop()
            )
            
            # Create channel
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=100)
            
            # Declare exchanges
            self.events_exchange = await self.channel.declare_exchange(
                "events",
                ExchangeType.TOPIC,
                durable=True
            )
            
            self.aggregations_exchange = await self.channel.declare_exchange(
                "aggregations",
                ExchangeType.TOPIC,
                durable=True
            )
            
            # Declare queues
            await self._setup_queues()
            
            self._initialized = True
            logger.info("Queue manager initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize queue manager", error=str(e))
            raise
    
    async def _setup_queues(self):
        """
        Setup required queues and bindings.
        Configura las colas requeridas y sus enlaces.
        """
        # Events queue for processing
        events_queue = await self.channel.declare_queue(
            "events.processing",
            durable=True,
            arguments={
                "x-message-ttl": 3600000,  # 1 hour TTL
                "x-max-length": 10000       # Max 10k messages
            }
        )
        
        # Bind to events exchange
        await events_queue.bind(
            self.events_exchange,
            routing_key="event.*"
        )
        
        # Aggregations queue
        aggregations_queue = await self.channel.declare_queue(
            "aggregations.processing",
            durable=True,
            arguments={
                "x-message-ttl": 7200000,  # 2 hour TTL
                "x-max-length": 5000        # Max 5k messages
            }
        )
        
        # Bind to aggregations exchange
        await aggregations_queue.bind(
            self.aggregations_exchange,
            routing_key="aggregation.*"
        )
        
        # Dead letter queue for failed messages
        dlq = await self.channel.declare_queue(
            "dead_letter_queue",
            durable=True,
            arguments={
                "x-message-ttl": 86400000,  # 24 hour TTL
                "x-max-length": 1000         # Max 1k messages
            }
        )
        
        logger.info("Queues configured successfully")
    
    async def close(self):
        """
        Close RabbitMQ connection.
        Cierra la conexión RabbitMQ.
        """
        if self.connection:
            await self.connection.close()
        
        logger.info("Queue manager connections closed")
    
    async def is_connected(self) -> bool:
        """Check if queue manager is connected / Verifica si el gestor de colas está conectado"""
        return self._initialized and self.connection and not self.connection.is_closed
    
    async def publish_event(self, event: DataEvent):
        """
        Publish an event to the message queue.
        Publica un evento a la cola de mensajes.
        """
        if not self.events_exchange:
            logger.error("Events exchange not initialized")
            return
        
        try:
            # Prepare message
            message_body = json.dumps(event.dict(), default=str)
            message = Message(
                body=message_body.encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                headers={
                    "event_id": event.event_id,
                    "source": event.source,
                    "event_type": event.event_type,
                    "timestamp": event.timestamp.isoformat()
                }
            )
            
            # Publish to exchange
            routing_key = f"event.{event.event_type}"
            await self.events_exchange.publish(
                message,
                routing_key=routing_key
            )
            
            logger.debug("Event published to queue",
                       event_id=event.event_id,
                       routing_key=routing_key)
            
        except Exception as e:
            logger.error("Failed to publish event",
                       event_id=event.event_id,
                       error=str(e))
            # Could implement retry logic here
    
    async def publish_aggregation(self, aggregation: AggregatedData):
        """
        Publish an aggregation to the message queue.
        Publica una agregación a la cola de mensajes.
        """
        if not self.aggregations_exchange:
            logger.error("Aggregations exchange not initialized")
            return
        
        try:
            # Prepare message
            message_body = json.dumps(aggregation.dict(), default=str)
            message = Message(
                body=message_body.encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                headers={
                    "aggregation_id": aggregation.aggregation_id,
                    "source": aggregation.source,
                    "event_type": aggregation.event_type,
                    "window_start": aggregation.window_start.isoformat(),
                    "window_end": aggregation.window_end.isoformat(),
                    "event_count": str(aggregation.count)
                }
            )
            
            # Publish to exchange
            routing_key = f"aggregation.{aggregation.event_type}"
            await self.aggregations_exchange.publish(
                message,
                routing_key=routing_key
            )
            
            logger.debug("Aggregation published to queue",
                       aggregation_id=aggregation.aggregation_id,
                       routing_key=routing_key)
            
        except Exception as e:
            logger.error("Failed to publish aggregation",
                       aggregation_id=aggregation.aggregation_id,
                       error=str(e))
    
    async def consume_events(self, callback):
        """
        Consume events from the queue.
        Consume eventos de la cola.
        """
        if not self.channel:
            logger.error("Channel not initialized")
            return
        
        # Get queue
        queue = await self.channel.get_queue("events.processing")
        
        # Start consuming
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        # Parse message
                        data = json.loads(message.body.decode())
                        event = DataEvent(**data)
                        
                        # Call callback
                        await callback(event)
                        
                    except Exception as e:
                        logger.error("Failed to process message",
                                   error=str(e),
                                   message=message.body.decode()[:200])
                        # Message will be requeued or sent to DLQ
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.
        Obtiene estadísticas de las colas.
        """
        if not self.channel:
            return {"error": "Channel not initialized"}
        
        try:
            stats = {}
            
            # Get events queue info - declare queue passively (won't create if doesn't exist)
            events_queue = await self.channel.declare_queue(
                "events.processing", 
                passive=True
            )
            stats["events_queue"] = {
                "message_count": events_queue.declaration_result.message_count,
                "consumer_count": events_queue.declaration_result.consumer_count
            }
            
            # Get aggregations queue info
            agg_queue = await self.channel.declare_queue(
                "aggregations.processing",
                passive=True
            )
            stats["aggregations_queue"] = {
                "message_count": agg_queue.declaration_result.message_count,
                "consumer_count": agg_queue.declaration_result.consumer_count
            }
            
            # Get DLQ info
            dlq = await self.channel.declare_queue(
                "dead_letter_queue",
                passive=True
            )
            stats["dead_letter_queue"] = {
                "message_count": dlq.declaration_result.message_count
            }
            
            return stats
            
        except Exception as e:
            logger.error("Failed to get queue stats", error=str(e))
            return {"error": str(e)}