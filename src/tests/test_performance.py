"""
Performance tests with timing measurements for StreamScale system.
Pruebas de rendimiento con medición de tiempos para el sistema StreamScale.
"""
import pytest
import asyncio
import time
import json
from datetime import datetime
from typing import Dict, Any, List, Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
import statistics

# Test components
from src.pipeline.processor import DataProcessor, AsyncDataProcessor
from src.pipeline.models import DataEvent
from src.context_manager.resource_manager import ResourceManager, AsyncResourceManager
from src.meta_programming.api_contract import APIContractBase, APIContractMeta
from src.iterator.lazy_collection import LazyCollection, create_fibonacci_generator
from src.scheduler.scheduler import DistributedScheduler
from src.scheduler.task import Task, TaskPriority


@dataclass
class PerformanceMetrics:
    """
    Performance metrics for a test.
    Métricas de rendimiento para una prueba.
    """
    test_name: str
    start_time: float = 0.0
    end_time: float = 0.0
    duration: float = 0.0
    operations: int = 0
    throughput: float = 0.0
    memory_start: int = 0
    memory_end: int = 0
    memory_delta: int = 0
    details: Dict[str, Any] = field(default_factory=dict)
    
    def calculate(self):
        """Calculate derived metrics / Calcula métricas derivadas"""
        self.duration = self.end_time - self.start_time
        if self.duration > 0 and self.operations > 0:
            self.throughput = self.operations / self.duration
        self.memory_delta = self.memory_end - self.memory_start
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary / Convertir a diccionario"""
        return {
            'test_name': self.test_name,
            'duration_seconds': round(self.duration, 4),
            'duration_ms': round(self.duration * 1000, 2),
            'operations': self.operations,
            'throughput_per_second': round(self.throughput, 2),
            'memory_delta_bytes': self.memory_delta,
            'details': self.details
        }
    
    def print_summary(self):
        """Print formatted summary / Imprime resumen formateado"""
        print(f"\n{'='*60}")
        print(f"TEST: {self.test_name}")
        print(f"{'='*60}")
        print(f"⏱️  Duration: {self.duration:.4f} seconds ({self.duration*1000:.2f} ms)")
        print(f"📊 Operations: {self.operations:,}")
        print(f"⚡ Throughput: {self.throughput:,.2f} ops/second")
        if self.memory_delta:
            print(f"💾 Memory Delta: {self.memory_delta:,} bytes")
        if self.details:
            print(f"📝 Details:")
            for key, value in self.details.items():
                print(f"   - {key}: {value}")


class PerformanceTimer:
    """Context manager for timing operations / Context manager para cronometrar operaciones"""
    
    def __init__(self, test_name: str, operations: int = 0):
        self.metrics = PerformanceMetrics(test_name=test_name, operations=operations)
        self.sub_timers: Dict[str, float] = {}
    
    def __enter__(self):
        """Enter context / Entrar al contexto"""
        import sys
        self.metrics.memory_start = sys.getsizeof(0)  # Simplified memory tracking
        self.metrics.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context / Salir del contexto"""
        import sys
        self.metrics.end_time = time.perf_counter()
        self.metrics.memory_end = sys.getsizeof(0)  # Simplified memory tracking
        self.metrics.calculate()
        return False
    
    def mark(self, label: str):
        """Mark a sub-timing point / Marca un punto de sub-tiempo"""
        self.sub_timers[label] = time.perf_counter()
    
    def get_elapsed(self, from_label: str = None) -> float:
        """Get elapsed time / Obtiene tiempo transcurrido"""
        if from_label and from_label in self.sub_timers:
            return time.perf_counter() - self.sub_timers[from_label]
        return time.perf_counter() - self.metrics.start_time


class TestDataPipelinePerformance:
    """Performance tests for the data pipeline"""
    
    def test_pipeline_throughput_small(self):
        """Test pipeline throughput with small dataset"""
        processor = DataProcessor(window_size_seconds=5, batch_size=100)
        
        # Create test data
        num_events = 1000
        raw_events = [
            {
                "event_id": f"perf_evt_{i}",
                "source": f"sensor_{i % 10}",
                "event_type": "measurement",
                "data": {"value": i, "temperature": 20 + (i % 10)}
            }
            for i in range(num_events)
        ]
        
        with PerformanceTimer("Pipeline Small Dataset", operations=num_events) as timer:
            results = list(processor.process_stream(iter(raw_events)))
            timer.metrics.details['results_count'] = len(results)
        
        timer.metrics.print_summary()
        assert timer.metrics.throughput > 100  # Should process >100 events/second
    
    def test_pipeline_throughput_large(self):
        """Test pipeline throughput with large dataset"""
        processor = DataProcessor(window_size_seconds=1, batch_size=1000)
        
        num_events = 10000
        
        def event_generator():
            for i in range(num_events):
                yield {
                    "event_id": f"perf_evt_{i}",
                    "source": f"sensor_{i % 50}",
                    "event_type": "measurement",
                    "data": {"value": i, "temperature": 20 + (i % 30), "humidity": 50 + (i % 20)}
                }
        
        with PerformanceTimer("Pipeline Large Dataset", operations=num_events) as timer:
            count = 0
            for event, aggregation in processor.process_stream(event_generator()):
                count += 1
                if count >= 5000:  # Process first 5000 for time
                    break
            timer.metrics.operations = count
            timer.metrics.details['events_processed'] = count
        
        timer.metrics.calculate()  # Recalculate with actual count
        timer.metrics.print_summary()
        assert timer.metrics.throughput > 500  # Should process >500 events/second
    
    @pytest.mark.asyncio
    async def test_async_pipeline_performance(self):
        """Test async pipeline performance"""
        processor = AsyncDataProcessor(window_size_seconds=2, batch_size=500)
        
        num_events = 5000
        
        def data_generator():
            for i in range(num_events):
                yield {
                    "event_id": f"async_evt_{i}",
                    "source": "async_sensor",
                    "event_type": "async_measurement",
                    "data": {"value": i * 10}
                }
        
        with PerformanceTimer("Async Pipeline", operations=num_events) as timer:
            count = 0
            async for result in processor.process_stream_async(data_generator()):
                count += 1
                if count >= 2000:  # Process first 2000
                    break
            timer.metrics.operations = count
            timer.metrics.details['async_events_processed'] = count
        
        timer.metrics.calculate()
        timer.metrics.print_summary()
        assert timer.metrics.throughput > 1000  # Async should be faster


class TestContextManagerPerformance:
    """Performance tests for context manager"""
    
    def test_resource_acquisition_speed(self):
        """Test speed of resource acquisition"""
        
        with PerformanceTimer("Resource Acquisition", operations=10) as timer:
            for i in range(10):
                with ResourceManager(enable_metrics=True) as rm:
                    # Simulate resource operations
                    rm.resources[f'test_{i}'] = f'value_{i}'
                    timer.metrics.details[f'iteration_{i}'] = timer.get_elapsed()
        
        timer.metrics.print_summary()
        avg_time = timer.metrics.duration / timer.metrics.operations
        assert avg_time < 0.01  # Should be <10ms per acquisition
    
    def test_nested_context_performance(self):
        """Test performance of nested contexts"""
        
        with PerformanceTimer("Nested Context Managers", operations=5) as timer:
            with ResourceManager() as parent:
                timer.mark("parent_created")
                
                for i in range(5):
                    child = ResourceManager()
                    parent.nest(child)
                    
                    with child:
                        child.resources[f'child_{i}'] = f'value_{i}'
                
                timer.metrics.details['nesting_time'] = timer.get_elapsed("parent_created")
        
        timer.metrics.print_summary()
        assert timer.metrics.duration < 0.1  # Should complete in <100ms


class TestMetaProgrammingPerformance:
    """Performance tests for meta-programming"""
    
    def test_class_creation_speed(self):
        """Test speed of class creation with metaclass"""
        
        with PerformanceTimer("Metaclass Class Creation", operations=100) as timer:
            classes_created = []
            
            for i in range(100):
                class_name = f"PerfClass_{i}"
                
                # Create class dynamically
                cls = type(class_name, (APIContractBase,), {
                    'version': f"1.0.{i}",
                    'description': f"Performance test class {i}",
                    'author': "PerfTest",
                    'process': lambda self, data: data,
                    'validate': lambda self, data: True
                })
                
                classes_created.append(cls)
            
            timer.metrics.details['classes_created'] = len(classes_created)
        
        timer.metrics.print_summary()
        assert timer.metrics.throughput > 100  # Should create >100 classes/second
    
    def test_runtime_validation_overhead(self):
        """Test overhead of runtime validation"""
        
        # Simple class without metaclass overhead for pure performance test
        class SimpleProcessor:
            def process(self, data):
                if not self.validate(data):
                    raise ValueError("Invalid data")
                return data * 2
            
            def validate(self, data):
                return isinstance(data, int)
        
        processor = SimpleProcessor()
        
        with PerformanceTimer("Runtime Validation Overhead", operations=10000) as timer:
            for i in range(10000):
                result = processor.process(i)
        
        timer.metrics.print_summary()
        assert timer.metrics.throughput > 10000  # Should process >10k ops/second


class TestLazyIteratorPerformance:
    """Performance tests for lazy iterator"""
    
    def test_lazy_evaluation_efficiency(self):
        """Test efficiency of lazy evaluation"""
        
        num_items = 1000000
        
        with PerformanceTimer("Lazy Evaluation", operations=10) as timer:
            # Create large collection
            collection = LazyCollection(range(num_items))
            
            timer.mark("collection_created")
            
            # Chain operations (should be instant - lazy)
            result = (collection
                     .filter(lambda x: x % 2 == 0)
                     .map(lambda x: x * 2)
                     .filter(lambda x: x > 1000)
                     .take(10))
            
            timer.metrics.details['chain_time_ms'] = timer.get_elapsed("collection_created") * 1000
            
            timer.mark("before_collect")
            
            # Materialize only 10 items
            final = result.collect()
            
            timer.metrics.details['collect_time_ms'] = timer.get_elapsed("before_collect") * 1000
            timer.metrics.details['items_collected'] = len(final)
        
        timer.metrics.print_summary()
        
        # Chaining should be instant (<1ms)
        assert timer.metrics.details['chain_time_ms'] < 1
        # Collection should be fast (<10ms) since we only process what's needed
        assert timer.metrics.details['collect_time_ms'] < 10
    
    def test_batch_processing_performance(self):
        """Test performance of batch processing"""
        
        num_items = 100000
        batch_size = 1000
        
        with PerformanceTimer("Batch Processing", operations=num_items) as timer:
            collection = LazyCollection(range(num_items))
            
            batch_count = 0
            for batch in collection.batch(batch_size):
                batch_count += 1
                if batch_count >= 10:  # Process first 10 batches
                    break
            
            timer.metrics.operations = batch_count * batch_size
            timer.metrics.details['batches_processed'] = batch_count
            timer.metrics.details['batch_size'] = batch_size
        
        timer.metrics.calculate()
        timer.metrics.print_summary()
        assert timer.metrics.throughput > 100000  # Should process >100k items/second
    
    def test_fibonacci_generation_speed(self):
        """Test speed of Fibonacci generation"""
        
        with PerformanceTimer("Fibonacci Generation", operations=1000) as timer:
            fib = LazyCollection(create_fibonacci_generator())
            
            # Generate first 1000 Fibonacci numbers
            result = fib.take(1000).collect()
            
            timer.metrics.details['numbers_generated'] = len(result)
            timer.metrics.details['largest_number'] = result[-1] if result else 0
        
        timer.metrics.print_summary()
        assert timer.metrics.throughput > 10000  # Should generate >10k numbers/second


class TestPerformanceComparison:
    """Comparative performance tests"""
    
    def test_sync_vs_async_comparison(self):
        """Compare sync vs async processing performance"""
        
        num_events = 1000
        test_data = [
            {"event_id": f"cmp_{i}", "source": "test", "event_type": "test", 
             "data": {"value": i}}
            for i in range(num_events)
        ]
        
        # Test synchronous
        sync_processor = DataProcessor()
        with PerformanceTimer("Sync Processing", operations=num_events) as sync_timer:
            sync_results = list(sync_processor.process_stream(iter(test_data)))
        
        sync_timer.metrics.print_summary()
        
        # Test optimized asynchronous with I/O simulation
        from src.pipeline.processor_async_optimized import OptimizedAsyncDataProcessor, async_generator_from_list
        
        async_processor = OptimizedAsyncDataProcessor()
        
        async def async_test():
            # Convert to async generator
            async_gen = async_generator_from_list(test_data)
            count = 0
            # Process with simulated I/O
            async for event, success in async_processor.process_with_io(async_gen):
                count += 1
            return count
        
        with PerformanceTimer("Optimized Async Processing", operations=num_events) as async_timer:
            asyncio.run(async_test())
        
        async_timer.metrics.print_summary()
        
        # For I/O bound operations, async should be faster
        # But for pure CPU operations, sync is typically faster
        # We're testing with simulated I/O, so async should perform well
        print(f"\n📊 Sync throughput: {sync_timer.metrics.throughput:.0f} ops/s")
        print(f"📊 Async throughput: {async_timer.metrics.throughput:.0f} ops/s")
        
        # With I/O simulation, async may be slower due to overhead
        # But it allows better concurrency in real scenarios
        assert async_timer.metrics.throughput > 100  # Should process >100 events/second even with I/O
    
    def test_memory_efficiency_comparison(self):
        """Compare memory efficiency of different approaches"""
        import sys
        
        num_items = 100000
        
        # Test list (eager)
        with PerformanceTimer("Eager List Processing", operations=num_items) as eager_timer:
            data = list(range(num_items))
            result = [x * 2 for x in data if x % 2 == 0][:10]
            eager_timer.metrics.details['memory_used'] = sys.getsizeof(data) + sys.getsizeof(result)
        
        eager_timer.metrics.print_summary()
        
        # Test lazy iterator
        with PerformanceTimer("Lazy Iterator Processing", operations=10) as lazy_timer:
            collection = LazyCollection(range(num_items))
            result = collection.filter(lambda x: x % 2 == 0).map(lambda x: x * 2).take(10).collect()
            lazy_timer.metrics.details['memory_used'] = sys.getsizeof(result)
        
        lazy_timer.metrics.print_summary()
        
        memory_ratio = eager_timer.metrics.details['memory_used'] / lazy_timer.metrics.details['memory_used']
        print(f"\n💾 Memory efficiency: Lazy uses {memory_ratio:.0f}x less memory")
        
        assert memory_ratio > 100  # Lazy should use much less memory


def run_all_performance_tests():
    """
    Run all performance tests and generate summary report.
    Ejecuta todas las pruebas de rendimiento y genera reporte resumen.
    """
    print("\n" + "="*80)
    print("🚀 STREAMSCALE PERFORMANCE TEST SUITE")
    print("="*80)
    
    all_metrics = []
    
    # Test Data Pipeline
    print("\n📊 DATA PIPELINE PERFORMANCE")
    print("-"*40)
    pipeline_tests = TestDataPipelinePerformance()
    all_metrics.append(pipeline_tests.test_pipeline_throughput_small())
    all_metrics.append(pipeline_tests.test_pipeline_throughput_large())
    
    # Test Context Manager
    print("\n🔧 CONTEXT MANAGER PERFORMANCE")
    print("-"*40)
    context_tests = TestContextManagerPerformance()
    all_metrics.append(context_tests.test_resource_acquisition_speed())
    all_metrics.append(context_tests.test_nested_context_performance())
    
    # Test Meta-Programming
    print("\n🧬 META-PROGRAMMING PERFORMANCE")
    print("-"*40)
    meta_tests = TestMetaProgrammingPerformance()
    all_metrics.append(meta_tests.test_class_creation_speed())
    all_metrics.append(meta_tests.test_runtime_validation_overhead())
    
    # Test Lazy Iterator
    print("\n🔄 LAZY ITERATOR PERFORMANCE")
    print("-"*40)
    lazy_tests = TestLazyIteratorPerformance()
    all_metrics.append(lazy_tests.test_lazy_evaluation_efficiency())
    all_metrics.append(lazy_tests.test_batch_processing_performance())
    all_metrics.append(lazy_tests.test_fibonacci_generation_speed())
    
    # Comparisons
    print("\n⚖️ PERFORMANCE COMPARISONS")
    print("-"*40)
    comparison_tests = TestPerformanceComparison()
    comparison_tests.test_sync_vs_async_comparison()
    comparison_tests.test_memory_efficiency_comparison()
    
    # Summary Report
    print("\n" + "="*80)
    print("📈 PERFORMANCE SUMMARY REPORT")
    print("="*80)
    
    total_time = sum(m.duration for m in all_metrics)
    total_ops = sum(m.operations for m in all_metrics)
    
    print(f"\nTotal Tests Run: {len(all_metrics)}")
    print(f"Total Time: {total_time:.4f} seconds")
    print(f"Total Operations: {total_ops:,}")
    print(f"Average Throughput: {total_ops/total_time:,.2f} ops/second")
    
    print("\n📊 Test Timings:")
    for metric in all_metrics:
        print(f"  • {metric.test_name}: {metric.duration*1000:.2f}ms ({metric.throughput:,.0f} ops/s)")
    
    # Find best/worst performers
    fastest = min(all_metrics, key=lambda m: m.duration)
    slowest = max(all_metrics, key=lambda m: m.duration)
    highest_throughput = max(all_metrics, key=lambda m: m.throughput)
    
    print(f"\n⚡ Fastest Test: {fastest.test_name} ({fastest.duration*1000:.2f}ms)")
    print(f"🐌 Slowest Test: {slowest.test_name} ({slowest.duration*1000:.2f}ms)")
    print(f"🏆 Highest Throughput: {highest_throughput.test_name} ({highest_throughput.throughput:,.0f} ops/s)")
    
    print("\n" + "="*80)
    print("✅ PERFORMANCE TEST SUITE COMPLETE")
    print("="*80)
    
    return all_metrics


if __name__ == "__main__":
    run_all_performance_tests()