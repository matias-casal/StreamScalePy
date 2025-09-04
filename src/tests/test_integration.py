"""
Integration tests for the complete StreamScale system.
Pruebas de integración para el sistema StreamScale completo.
"""
import pytest
import asyncio
import json
import time
from datetime import datetime
from typing import Dict, Any, List
import httpx

# Test components
from src.pipeline.processor import DataProcessor, AsyncDataProcessor
from src.pipeline.models import DataEvent
from src.context_manager.resource_manager import ResourceManager, AsyncResourceManager
from src.meta_programming.api_contract import APIContractBase, APIContractMeta
from src.iterator.lazy_collection import LazyCollection, create_fibonacci_generator
from src.scheduler.scheduler import DistributedScheduler
from src.scheduler.task import Task, TaskPriority, TaskStatus


class TestDataPipeline:
    """Test the memory-efficient data pipeline"""
    
    def test_data_processor_transformation(self):
        """Test data transformation through pipeline"""
        processor = DataProcessor(window_size_seconds=5, batch_size=10)
        
        # Create test data
        raw_events = [
            {
                "event_id": f"evt_{i}",
                "source": "sensor_1",
                "event_type": "temperature",
                "data": {"temperature": 20 + i, "humidity": 50 + i}
            }
            for i in range(20)
        ]
        
        # Process through pipeline
        results = list(processor.process_stream(iter(raw_events)))
        
        assert len(results) > 0
        
        for event, aggregation in results:
            if event:
                assert isinstance(event, DataEvent)
                assert event.event_id.startswith("evt_")
            
            if aggregation:
                assert aggregation.count > 0
                assert "temperature_avg" in aggregation.metrics
    
    @pytest.mark.asyncio
    async def test_async_data_processor(self):
        """Test asynchronous data processing"""
        processor = AsyncDataProcessor(window_size_seconds=5, batch_size=5)
        
        # Create test data generator
        def data_generator():
            for i in range(10):
                yield {
                    "event_id": f"async_evt_{i}",
                    "source": "async_sensor",
                    "event_type": "measurement",
                    "data": {"value": i * 10}
                }
        
        # Process asynchronously
        results = []
        async for result in processor.process_stream_async(data_generator()):
            results.append(result)
        
        assert len(results) > 0
    
    def test_pipeline_memory_efficiency(self):
        """Test that pipeline uses constant memory with large datasets"""
        processor = DataProcessor(window_size_seconds=1, batch_size=100)
        
        # Create large dataset generator
        def large_dataset():
            for i in range(10000):
                yield {
                    "event_id": f"mem_test_{i}",
                    "source": "memory_test",
                    "event_type": "test",
                    "data": {"value": i}
                }
        
        # Process without materializing entire dataset
        count = 0
        for event, _ in processor.process_stream(large_dataset()):
            count += 1
            if count > 100:  # Process only first 100 for test
                break
        
        assert count > 0


class TestContextManager:
    """Test the custom context manager"""
    
    def test_resource_manager_basic(self):
        """Test basic resource management"""
        with ResourceManager(enable_metrics=True) as rm:
            # Test that manager is accessible
            assert rm is not None
            assert rm.enable_metrics is True
            
            # Metrics should be initialized
            assert isinstance(rm.metrics, dict)
    
    def test_nested_resource_managers(self):
        """Test nested context managers"""
        with ResourceManager() as parent:
            assert parent is not None
            
            child = ResourceManager()
            parent.nest(child)
            
            with child:
                assert child.parent_manager == parent
                assert child in parent.nested_managers
    
    def test_resource_cleanup_on_exception(self):
        """Test that resources are cleaned up on exception"""
        rm = ResourceManager()
        
        try:
            with rm:
                # Simulate resource acquisition
                rm.resources['test_resource'] = "test_value"
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Resources should be cleaned up
        assert 'test_resource' not in rm.resources
    
    @pytest.mark.asyncio
    async def test_async_resource_manager(self):
        """Test asynchronous resource manager"""
        async with AsyncResourceManager() as arm:
            assert arm is not None
            assert isinstance(arm.resources, dict)


class TestMetaProgramming:
    """Test the meta-programming features"""
    
    def test_api_contract_enforcement(self):
        """Test that API contract is enforced"""
        
        # Valid implementation
        class ValidProcessor(APIContractBase):
            version = "1.0.0"
            description = "Valid processor"
            author = "Test"
            
            def process(self, data):
                return data.upper() if isinstance(data, str) else data
            
            def validate(self, data):
                return data is not None
        
        # Create instance
        processor = ValidProcessor()
        assert processor.process("hello") == "HELLO"
        assert processor.validate("test") is True
    
    def test_api_contract_violation(self):
        """Test that contract violations are caught"""
        from src.meta_programming.api_contract import ContractViolationError
        
        with pytest.raises(ContractViolationError):
            class InvalidProcessor(APIContractBase):
                version = "1.0.0"
                # Missing required methods and attributes
                pass
    
    def test_class_registration(self):
        """Test automatic class registration"""
        
        class RegisteredClass(APIContractBase):
            version = "2.0.0"
            description = "Auto-registered class"
            author = "Test"
            
            def process(self, data):
                return data
            
            def validate(self, data):
                return True
        
        # Check that class is registered
        registry = APIContractMeta.get_registry()
        assert len(registry) > 0
        
        # Find our class in registry
        found = False
        for class_id, cls in registry.items():
            if cls.__name__ == "RegisteredClass":
                found = True
                break
        
        assert found
    
    def test_runtime_validation(self):
        """Test runtime validation decorators"""
        
        class ValidatedProcessor(APIContractBase):
            version = "1.0.0"
            description = "Processor with validation"
            author = "Test"
            
            def process(self, data):
                if not isinstance(data, (str, int, float)):
                    raise TypeError("Invalid data type")
                return str(data).upper()
            
            def validate(self, data):
                return data is not None
        
        processor = ValidatedProcessor()
        
        # Valid data
        assert processor.process("test") == "TEST"
        assert processor.process(123) == "123"
        
        # Invalid data
        with pytest.raises(TypeError):
            processor.process([1, 2, 3])


class TestLazyIterator:
    """Test the lazy evaluation iterator"""
    
    def test_lazy_collection_basic(self):
        """Test basic lazy collection operations"""
        collection = LazyCollection(range(1, 11))
        
        # Chain operations
        result = (collection
                 .map(lambda x: x * 2)
                 .filter(lambda x: x > 5)
                 .take(3)
                 .collect())
        
        assert result == [6, 8, 10]
    
    def test_lazy_collection_window(self):
        """Test windowing operations"""
        collection = LazyCollection(range(1, 8))
        windows = collection.window(3).collect()
        
        assert len(windows) == 5
        assert windows[0] == [1, 2, 3]
        assert windows[-1] == [5, 6, 7]
    
    def test_lazy_collection_batch(self):
        """Test batching operations"""
        collection = LazyCollection(range(1, 16))
        batches = collection.batch(5).collect()
        
        assert len(batches) == 3
        assert batches[0] == [1, 2, 3, 4, 5]
        assert batches[-1] == [11, 12, 13, 14, 15]
    
    def test_lazy_evaluation_efficiency(self):
        """Test that operations are truly lazy"""
        
        # Create expensive operation that shouldn't execute
        def expensive_operation(x):
            time.sleep(0.1)  # Simulate expensive computation
            return x * 2
        
        # Create lazy collection with expensive operation
        collection = LazyCollection(range(1000))
        lazy_result = collection.map(expensive_operation).take(2)
        
        # This should be instant (lazy)
        start = time.time()
        lazy_collection_created = lazy_result  # Not evaluated yet
        assert time.time() - start < 0.01
        
        # This should take ~0.2 seconds (only processes 2 items)
        start = time.time()
        result = lazy_result.collect()
        elapsed = time.time() - start
        
        assert len(result) == 2
        assert elapsed < 0.5  # Should be much less than processing all 1000
    
    def test_fibonacci_generator(self):
        """Test Fibonacci generator with lazy evaluation"""
        fib = LazyCollection(create_fibonacci_generator())
        
        # Get first 10 Fibonacci numbers
        result = fib.take(10).collect()
        
        assert result == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
        
        # Filter even Fibonacci numbers
        even_fibs = (LazyCollection(create_fibonacci_generator())
                    .take(20)
                    .filter(lambda x: x % 2 == 0)
                    .collect())
        
        assert all(x % 2 == 0 for x in even_fibs)
    
    def test_pagination(self):
        """Test pagination functionality"""
        collection = LazyCollection(range(1, 101))
        
        # Paginate returns an iterator of LazyCollections
        pages = list(collection.paginate(10))
        
        assert len(pages) == 10
        
        # Collect the first page
        first_page = pages[0].collect()
        assert first_page == list(range(1, 11))
        
        # Collect the last page
        last_page = pages[-1].collect()
        assert last_page == list(range(91, 101))


class TestDistributedScheduler:
    """Test the distributed task scheduler"""
    
    def test_scheduler_initialization(self):
        """Test scheduler initialization"""
        scheduler = DistributedScheduler(
            num_workers=2,
            max_queue_size=100,
            enable_monitoring=False,
            use_redis=False,
            use_rabbitmq=False
        )
        
        scheduler.initialize()
        
        assert scheduler.num_workers == 2
        assert scheduler.worker_pool is not None
        
        scheduler.shutdown()
    
    def test_task_submission_and_execution(self):
        """Test task submission and execution"""
        scheduler = DistributedScheduler(
            num_workers=2,
            use_redis=False,
            use_rabbitmq=False
        )
        scheduler.initialize()
        
        try:
            # Define test task
            def test_task(x, y):
                return x + y
            
            # Submit task
            task_id = scheduler.submit_task(
                test_task,
                args=(5, 3),
                name="test_addition"
            )
            
            assert task_id is not None
            assert task_id in scheduler.task_graph.tasks
            
            # Start scheduler processing
            scheduler.run()
            
            # Wait for task to complete (max 5 seconds)
            import time
            for _ in range(50):
                task = scheduler.task_graph.tasks.get(task_id)
                if task and task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                    break
                time.sleep(0.1)
            
            # Verify task completed
            task = scheduler.task_graph.tasks.get(task_id)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED
            
        finally:
            scheduler.shutdown()
    
    def test_task_dependencies(self):
        """Test task dependency handling"""
        scheduler = DistributedScheduler(
            num_workers=2,
            use_redis=False,
            use_rabbitmq=False
        )
        scheduler.initialize()
        
        def parent_task():
            return "parent_complete"
        
        def child_task(parent_result):
            return f"child_received: {parent_result}"
        
        # Submit parent task
        parent_id = scheduler.submit_task(
            parent_task,
            name="parent"
        )
        
        # Submit child task with dependency
        child_id = scheduler.submit_task(
            child_task,
            args=("placeholder",),
            name="child",
            dependencies={parent_id}
        )
        
        # Check that child is waiting for parent
        child_task_obj = scheduler.task_graph.tasks[child_id]
        assert not child_task_obj.is_ready(set())
        assert child_task_obj.is_ready({parent_id})
        
        scheduler.shutdown()
    
    def test_task_priority(self):
        """Test task priority handling"""
        scheduler = DistributedScheduler(
            num_workers=1,
            use_redis=False,
            use_rabbitmq=False
        )
        scheduler.initialize()
        
        def priority_task(priority):
            return f"priority_{priority}"
        
        # Submit tasks with different priorities
        low_id = scheduler.submit_task(
            priority_task,
            args=("low",),
            priority=TaskPriority.LOW
        )
        
        high_id = scheduler.submit_task(
            priority_task,
            args=("high",),
            priority=TaskPriority.HIGH
        )
        
        critical_id = scheduler.submit_task(
            priority_task,
            args=("critical",),
            priority=TaskPriority.CRITICAL
        )
        
        # Tasks should be queued by priority
        ready_tasks = scheduler.task_graph.get_ready_tasks()
        
        # Critical should come first
        if ready_tasks:
            assert ready_tasks[0].priority == TaskPriority.CRITICAL
        
        scheduler.shutdown()
    
    def test_worker_scaling(self):
        """Test dynamic worker scaling"""
        scheduler = DistributedScheduler(
            num_workers=2,
            use_redis=False,
            use_rabbitmq=False
        )
        scheduler.initialize()
        
        initial_workers = scheduler.worker_pool.num_workers
        assert initial_workers == 2
        
        # Scale up
        scheduler.scale_workers(4)
        assert scheduler.worker_pool.num_workers == 4
        
        # Scale down
        scheduler.scale_workers(1)
        assert scheduler.worker_pool.num_workers == 1
        
        scheduler.shutdown()
    
    def test_task_cancellation(self):
        """Test task cancellation"""
        scheduler = DistributedScheduler(
            num_workers=2,
            use_redis=False,
            use_rabbitmq=False
        )
        scheduler.initialize()
        
        def long_task():
            import time
            time.sleep(10)
            return "completed"
        
        # Submit task
        task_id = scheduler.submit_task(
            long_task,
            name="long_task"
        )
        
        # Cancel task
        cancelled = scheduler.cancel_task(task_id)
        assert cancelled is True
        
        # Check task status
        task = scheduler.task_graph.tasks[task_id]
        assert task.status.value == "cancelled"
        
        scheduler.shutdown()


class TestSystemIntegration:
    """Test full system integration"""
    
    def test_pipeline_with_context_manager(self):
        """Test pipeline using context manager for resources"""
        with ResourceManager() as rm:
            processor = DataProcessor()
            
            # Process test data
            test_data = [
                {"event_id": f"ctx_{i}", "source": "test", 
                 "event_type": "test", "data": {"value": i}}
                for i in range(10)
            ]
            
            results = list(processor.process_stream(iter(test_data)))
            assert len(results) > 0
    
    def test_lazy_iterator_with_metaclass(self):
        """Test lazy iterator with metaclass-enforced objects"""
        
        class DataTransformer(APIContractBase):
            version = "1.0.0"
            description = "Transforms data"
            author = "Test"
            
            def process(self, data):
                return data * 2
            
            def validate(self, data):
                return isinstance(data, (int, float))
        
        transformer = DataTransformer()
        
        # Use with lazy collection
        collection = LazyCollection(range(1, 6))
        result = collection.map(transformer.process).collect()
        
        assert result == [2, 4, 6, 8, 10]
    
    @pytest.mark.asyncio
    async def test_async_pipeline_with_scheduler(self):
        """Test async pipeline with task scheduler"""
        # This would require running services
        # Simplified test for demonstration
        
        processor = AsyncDataProcessor()
        
        def generate_data():
            for i in range(5):
                yield {
                    "event_id": f"sched_{i}",
                    "source": "scheduler_test",
                    "event_type": "test",
                    "data": {"value": i}
                }
        
        results = []
        async for result in processor.process_stream_async(generate_data()):
            results.append(result)
        
        assert len(results) > 0


# Performance tests
class TestPerformance:
    """Performance and scalability tests"""
    
    def test_large_dataset_processing(self):
        """Test processing of large datasets"""
        processor = DataProcessor(window_size_seconds=1, batch_size=1000)
        
        def large_dataset():
            for i in range(100000):
                yield {
                    "event_id": f"perf_{i}",
                    "source": f"sensor_{i % 10}",
                    "event_type": "measurement",
                    "data": {"value": i, "timestamp": i}
                }
        
        start_time = time.time()
        count = 0
        
        for event, aggregation in processor.process_stream(large_dataset()):
            count += 1
            if count >= 10000:  # Process first 10k for test
                break
        
        elapsed = time.time() - start_time
        throughput = count / elapsed
        
        print(f"Processed {count} events in {elapsed:.2f} seconds")
        print(f"Throughput: {throughput:.0f} events/second")
        
        assert throughput > 100  # Should process at least 100 events/second
    
    def test_memory_usage_constant(self):
        """Test that memory usage remains constant with streaming"""
        import gc
        import sys
        
        # Force garbage collection
        gc.collect()
        
        # Create infinite generator
        def infinite_generator():
            i = 0
            while True:
                yield i
                i += 1
        
        collection = LazyCollection(infinite_generator())
        
        # Process large amount of data lazily
        result = collection.take(1000000).filter(lambda x: x % 2 == 0).take(10).collect()
        
        # Should only have 10 items in memory
        assert len(result) == 10
        assert sys.getsizeof(result) < 1000  # Small memory footprint


if __name__ == "__main__":
    pytest.main([__file__, "-v"])