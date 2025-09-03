"""
Main entry point for the distributed scheduler service.
Punto de entrada principal para el servicio de planificador distribuido.
"""
import asyncio
import signal
import sys
from typing import Any

from src.scheduler.scheduler import DistributedScheduler
from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def example_task(x: int, y: int) -> int:
    """Example task for testing / Tarea de ejemplo para pruebas"""
    return x + y


def complex_task(n: int) -> int:
    """Complex task that takes time / Tarea compleja que toma tiempo"""
    import time
    time.sleep(1)  # Simulate work
    return n * n


async def main():
    """
    Main function to run the scheduler.
    Función principal para ejecutar el planificador.
    """
    logger.info("Starting StreamScale Distributed Scheduler")
    
    # Create scheduler
    scheduler = DistributedScheduler(
        num_workers=settings.scheduler.workers,
        max_queue_size=settings.scheduler.max_tasks,
        enable_monitoring=True,
        use_redis=True,
        use_rabbitmq=True
    )
    
    try:
        # Initialize scheduler
        scheduler.initialize()
        
        # Submit some example tasks
        logger.info("Submitting example tasks")
        
        task_ids = []
        for i in range(5):
            task_id = scheduler.submit_task(
                example_task,
                args=(i, i + 1),
                name=f"example_task_{i}"
            )
            task_ids.append(task_id)
        
        # Submit tasks with dependencies
        parent_id = scheduler.submit_task(
            complex_task,
            args=(10,),
            name="parent_task"
        )
        
        child_id = scheduler.submit_task(
            complex_task,
            args=(20,),
            name="child_task",
            dependencies={parent_id}
        )
        
        logger.info(f"Submitted {len(task_ids) + 2} tasks")
        
        # Run scheduler
        logger.info("Running scheduler main loop")
        scheduler.run()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}")
    finally:
        # Shutdown scheduler
        logger.info("Shutting down scheduler")
        scheduler.shutdown()
        logger.info("Scheduler shutdown complete")


if __name__ == "__main__":
    # Run async main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)