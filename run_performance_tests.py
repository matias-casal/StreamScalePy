#!/usr/bin/env python3
"""
Simplified performance test runner for StreamScale components.
"""
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipeline.processor import DataProcessor
from src.context_manager.resource_manager import ResourceManager
from src.iterator.lazy_collection import LazyCollection, create_fibonacci_generator

def suppress_logs():
    """Suppress logs for cleaner output"""
    import logging
    logging.basicConfig(level=logging.CRITICAL)
    # Disable structlog
    import structlog
    structlog.configure(
        processors=[],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=open(os.devnull, 'w')),
        cache_logger_on_first_use=True,
    )

suppress_logs()

def measure_time(func, name, iterations=1):
    """Measure execution time of a function"""
    start = time.perf_counter()
    result = func()
    end = time.perf_counter()
    duration = end - start
    
    print(f"\n{'='*60}")
    print(f"⚙️  TEST: {name}")
    print(f"{'='*60}")
    print(f"⏱️  Duration: {duration:.4f} seconds ({duration*1000:.2f} ms)")
    if iterations > 1:
        print(f"📊 Operations: {iterations:,}")
        print(f"⚡ Throughput: {iterations/duration:,.2f} ops/second")
    
    return duration

def test_data_pipeline():
    """Test data pipeline performance"""
    processor = DataProcessor(window_size_seconds=5, batch_size=100)
    
    # Create test data
    num_events = 5000
    raw_events = [
        {
            "event_id": f"evt_{i}",
            "source": f"sensor_{i % 10}",
            "event_type": "measurement",
            "data": {"value": i, "temperature": 20 + (i % 10)}
        }
        for i in range(num_events)
    ]
    
    def process():
        count = 0
        for event, aggregation in processor.process_stream(iter(raw_events)):
            count += 1
            if count >= 1000:
                break
        return count
    
    result = measure_time(process, "Data Pipeline (1000 events)", 1000)
    return result

def test_context_manager():
    """Test context manager performance"""
    
    def acquire_resources():
        operations = 0
        for i in range(100):
            with ResourceManager(enable_metrics=False, enable_logging=False) as rm:
                rm.resources[f'test_{i}'] = f'value_{i}'
                operations += 1
        return operations
    
    result = measure_time(acquire_resources, "Context Manager (100 acquisitions)", 100)
    return result

def test_lazy_iterator():
    """Test lazy iterator performance"""
    
    def process_lazy():
        # Create large collection
        collection = LazyCollection(range(1000000))
        
        # Chain operations (should be lazy and fast)
        result = (collection
                 .filter(lambda x: x % 2 == 0)
                 .map(lambda x: x * 2)
                 .filter(lambda x: x > 1000)
                 .take(100)
                 .collect())
        
        return len(result)
    
    result = measure_time(process_lazy, "Lazy Iterator (1M items → 100)", 100)
    return result

def test_fibonacci():
    """Test Fibonacci generation"""
    
    def generate_fib():
        fib = LazyCollection(create_fibonacci_generator())
        result = fib.take(1000).collect()
        return len(result)
    
    result = measure_time(generate_fib, "Fibonacci Generation (1000 numbers)", 1000)
    return result

def test_batch_processing():
    """Test batch processing performance"""
    
    def process_batches():
        collection = LazyCollection(range(100000))
        batch_count = 0
        for batch in collection.batch(1000):
            batch_count += 1
            if batch_count >= 50:
                break
        return batch_count * 1000
    
    result = measure_time(process_batches, "Batch Processing (50k items)", 50000)
    return result

def compare_eager_vs_lazy():
    """Compare eager vs lazy evaluation"""
    num_items = 100000
    
    print(f"\n{'='*60}")
    print(f"⚖️  COMPARISON: Eager vs Lazy Evaluation")
    print(f"{'='*60}")
    
    # Eager evaluation
    start = time.perf_counter()
    data = list(range(num_items))
    result_eager = [x * 2 for x in data if x % 2 == 0][:10]
    eager_time = time.perf_counter() - start
    
    print(f"📌 Eager List Processing:")
    print(f"   Time: {eager_time*1000:.2f} ms")
    print(f"   Memory: ~{sys.getsizeof(data) / 1024:.0f} KB for data list")
    
    # Lazy evaluation
    start = time.perf_counter()
    collection = LazyCollection(range(num_items))
    result_lazy = collection.filter(lambda x: x % 2 == 0).map(lambda x: x * 2).take(10).collect()
    lazy_time = time.perf_counter() - start
    
    print(f"📌 Lazy Iterator Processing:")
    print(f"   Time: {lazy_time*1000:.2f} ms")
    print(f"   Memory: ~{sys.getsizeof(result_lazy) / 1024:.2f} KB for result only")
    
    speedup = eager_time / lazy_time
    print(f"\n🚀 Lazy is {speedup:.1f}x faster for this operation")

def main():
    print("\n" + "="*60)
    print("🚀 STREAMSCALE PERFORMANCE MEASUREMENTS")
    print("="*60)
    
    all_times = []
    
    # Run tests
    all_times.append(("Data Pipeline", test_data_pipeline()))
    all_times.append(("Context Manager", test_context_manager()))
    all_times.append(("Lazy Iterator", test_lazy_iterator()))
    all_times.append(("Fibonacci", test_fibonacci()))
    all_times.append(("Batch Processing", test_batch_processing()))
    
    # Comparison
    compare_eager_vs_lazy()
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📈 PERFORMANCE SUMMARY")
    print(f"{'='*60}")
    
    total_time = sum(t[1] for t in all_times)
    print(f"\n📊 Component Timings:")
    for name, duration in all_times:
        print(f"   • {name}: {duration*1000:.2f} ms")
    
    print(f"\n⏱️  Total Time: {total_time:.4f} seconds")
    
    # Find best performers
    fastest = min(all_times, key=lambda x: x[1])
    slowest = max(all_times, key=lambda x: x[1])
    
    print(f"⚡ Fastest: {fastest[0]} ({fastest[1]*1000:.2f} ms)")
    print(f"🐌 Slowest: {slowest[0]} ({slowest[1]*1000:.2f} ms)")
    
    print(f"\n✅ All performance tests completed successfully!")

if __name__ == "__main__":
    main()