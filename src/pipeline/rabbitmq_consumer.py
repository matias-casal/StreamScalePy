"""
RabbitMQ Consumer for processing queued events.
Consumidor de RabbitMQ para procesar eventos encolados.
"""
import json
import threading
import time
from typing import Optional, Callable, Dict, Any
import pika
import pika.exceptions
from src.shared.logging import get_logger
from src.shared.config import settings
from src.pipeline.processor_optimized import OptimizedDataProcessor
from src.pipeline.storage import DataStorage

logger = get_logger(__name__)


class RabbitMQConsumer:
    """
    Consumer for processing messages from RabbitMQ queues.
    Consumidor para procesar mensajes de colas RabbitMQ.
    """
    
    def __init__(self, queue_name: str, processor: Optional[Callable] = None):
        """
        Initialize consumer.
        
        Args:
            queue_name: Name of the queue to consume from
            processor: Optional message processor function
        """
        self.queue_name = queue_name
        self.processor = processor
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.consuming = False
        self.consumer_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        
        # Initialize processor and storage if not provided
        if not self.processor:
            self.data_processor = OptimizedDataProcessor()
            self.storage = DataStorage()
            
    def connect(self) -> bool:
        """
        Establish connection to RabbitMQ.
        Establece conexión con RabbitMQ.
        """
        try:
            parameters = pika.ConnectionParameters(
                host=settings.rabbitmq.host,
                port=settings.rabbitmq.port,
                virtual_host='/',
                credentials=pika.PlainCredentials(
                    settings.rabbitmq.user,
                    settings.rabbitmq.password
                ),
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=3,
                retry_delay=2
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare queue (idempotent operation)
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                arguments={'x-max-length': 100000}  # Max 100k messages
            )
            
            # Set QoS
            self.channel.basic_qos(prefetch_count=10)
            
            logger.info(f"Connected to RabbitMQ queue: {self.queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            return False
    
    def process_message(self, channel, method, properties, body):
        """
        Process a single message from the queue.
        Procesa un único mensaje de la cola.
        """
        try:
            # Decode message
            if isinstance(body, bytes):
                body = body.decode('utf-8')
            
            data = json.loads(body)
            
            # Process with custom processor or default
            if self.processor:
                result = self.processor(data)
            else:
                # Use default processing pipeline
                result = self._default_process(data)
            
            # Acknowledge message
            channel.basic_ack(delivery_tag=method.delivery_tag)
            
            logger.debug(f"Processed message from {self.queue_name}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {str(e)}")
            # Reject and don't requeue malformed messages
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Requeue for retry
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def _default_process(self, data: Dict[str, Any]) -> bool:
        """
        Default processing pipeline for events.
        Pipeline de procesamiento por defecto para eventos.
        """
        try:
            # Transform event
            event = self.data_processor.transform_event_fast(data)
            if not event:
                return False
            
            # Store event
            stored_event = self.storage.store_event(event)
            
            # Check for aggregation
            if stored_event and hasattr(stored_event, 'id'):
                # Simple aggregation check (every 100 events)
                if stored_event.id % 100 == 0:
                    # Trigger aggregation for the source
                    aggregations = self.storage.get_aggregations_by_source(
                        event.source,
                        limit=1
                    )
                    logger.info(f"Aggregation check for source {event.source}")
            
            return True
            
        except Exception as e:
            logger.error(f"Default processing failed: {str(e)}")
            return False
    
    def start_consuming(self):
        """
        Start consuming messages in a separate thread.
        Inicia el consumo de mensajes en un hilo separado.
        """
        if self.consuming:
            logger.warning(f"Consumer for {self.queue_name} already running")
            return
        
        self.consuming = True
        self.consumer_thread = threading.Thread(
            target=self._consume_loop,
            name=f"Consumer-{self.queue_name}"
        )
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
        logger.info(f"Started consumer for queue: {self.queue_name}")
    
    def _consume_loop(self):
        """
        Main consumption loop.
        Bucle principal de consumo.
        """
        while not self.shutdown_event.is_set():
            try:
                # Connect if not connected
                if not self.connection or self.connection.is_closed:
                    if not self.connect():
                        time.sleep(5)  # Retry after 5 seconds
                        continue
                
                # Start consuming
                self.channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=self.process_message,
                    auto_ack=False
                )
                
                logger.info(f"Consuming from queue: {self.queue_name}")
                
                # Process messages
                while not self.shutdown_event.is_set():
                    try:
                        self.connection.process_data_events(time_limit=1)
                    except pika.exceptions.ConnectionClosed:
                        logger.warning("Connection closed, reconnecting...")
                        break
                    except Exception as e:
                        logger.error(f"Error in consume loop: {str(e)}")
                        break
                        
            except Exception as e:
                logger.error(f"Consumer loop error: {str(e)}")
                time.sleep(5)  # Wait before retry
        
        logger.info(f"Consumer for {self.queue_name} stopped")
    
    def stop_consuming(self):
        """
        Stop consuming messages.
        Detiene el consumo de mensajes.
        """
        logger.info(f"Stopping consumer for {self.queue_name}")
        
        self.consuming = False
        self.shutdown_event.set()
        
        # Close connection
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except:
                pass
        
        # Wait for thread to finish
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)
        
        logger.info(f"Consumer for {self.queue_name} stopped")
    
    def get_queue_size(self) -> Optional[int]:
        """
        Get current queue size.
        Obtiene el tamaño actual de la cola.
        """
        try:
            if self.channel and self.connection and not self.connection.is_closed:
                method = self.channel.queue_declare(
                    queue=self.queue_name,
                    passive=True
                )
                return method.method.message_count
        except:
            pass
        return None


class ConsumerManager:
    """
    Manager for multiple RabbitMQ consumers.
    Gestor para múltiples consumidores de RabbitMQ.
    """
    
    def __init__(self):
        """Initialize consumer manager."""
        self.consumers: Dict[str, RabbitMQConsumer] = {}
        
    def add_consumer(self, queue_name: str, processor: Optional[Callable] = None) -> RabbitMQConsumer:
        """
        Add and start a new consumer.
        Añade e inicia un nuevo consumidor.
        """
        if queue_name in self.consumers:
            logger.warning(f"Consumer for {queue_name} already exists")
            return self.consumers[queue_name]
        
        consumer = RabbitMQConsumer(queue_name, processor)
        consumer.start_consuming()
        self.consumers[queue_name] = consumer
        
        return consumer
    
    def remove_consumer(self, queue_name: str):
        """
        Stop and remove a consumer.
        Detiene y elimina un consumidor.
        """
        if queue_name in self.consumers:
            self.consumers[queue_name].stop_consuming()
            del self.consumers[queue_name]
            logger.info(f"Removed consumer for {queue_name}")
    
    def start_all(self):
        """
        Start all consumers.
        Inicia todos los consumidores.
        """
        for consumer in self.consumers.values():
            if not consumer.consuming:
                consumer.start_consuming()
    
    def stop_all(self):
        """
        Stop all consumers.
        Detiene todos los consumidores.
        """
        for consumer in self.consumers.values():
            consumer.stop_consuming()
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get status of all consumers.
        Obtiene el estado de todos los consumidores.
        """
        status = {}
        for queue_name, consumer in self.consumers.items():
            status[queue_name] = {
                'consuming': consumer.consuming,
                'queue_size': consumer.get_queue_size(),
                'connected': consumer.connection and not consumer.connection.is_closed
            }
        return status


# Global consumer manager instance
consumer_manager = ConsumerManager()


def start_default_consumers():
    """
    Start default consumers for the system.
    Inicia los consumidores por defecto del sistema.
    """
    # Add consumers for main queues
    consumer_manager.add_consumer('events.processing')
    consumer_manager.add_consumer('aggregations.processing')
    consumer_manager.add_consumer('task_queue')
    
    logger.info("Default consumers started")


def stop_all_consumers():
    """
    Stop all active consumers.
    Detiene todos los consumidores activos.
    """
    consumer_manager.stop_all()
    logger.info("All consumers stopped")