"""
Standalone worker service for distributed task execution.
Servicio worker independiente para ejecución distribuida de tareas.
"""
import asyncio
import signal
import sys
from src.scheduler.worker import Worker
from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def main():
    """
    Main function for standalone worker service.
    Función principal para servicio worker independiente.
    """
    logger.info("Starting StreamScale Worker Service")
    
    # Create worker
    worker = Worker()
    
    try:
        # Start worker
        worker.start()
        logger.info(f"Worker {worker.worker_id} started")
        
        # Keep running until interrupted
        signal.pause()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Worker error: {str(e)}")
    finally:
        # Shutdown worker
        logger.info("Shutting down worker")
        worker.shutdown()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    main()