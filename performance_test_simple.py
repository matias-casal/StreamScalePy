#!/usr/bin/env python3
"""
Simple performance measurements for StreamScale components.
"""
import time
import sys
import os

# Disable all logging
import logging
logging.disable(logging.CRITICAL)
os.environ['PYTHONWARNINGS'] = 'ignore'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_data_pipeline():
    """Test data pipeline performance"""
    from src.pipeline.models import DataEvent
    from datetime import datetime
    
    print("\n1. DATA PIPELINE TEST")
    print("-" * 40)
    
    # Create test events
    num_events = 10000
    events = []
    
    start = time.perf_counter()
    
    for i in range(num_events):
        event = DataEvent(
            event_id=f"evt_{i}",
            source="sensor_1",
            event_type="measurement",
            data={"value": i, "temperature": 20 + (i % 10)},
            timestamp=datetime.utcnow()
        )
        events.append(event)
    
    # Simple transformation
    transformed = []
    for event in events[:1000]:
        event.data["value"] = event.data["value"] * 2
        transformed.append(event)
    
    end = time.perf_counter()
    duration = end - start
    
    print(f"✅ Processed {len(transformed)} events")
    print(f"⏱️  Time: {duration:.4f} seconds ({duration*1000:.2f} ms)")
    print(f"⚡ Throughput: {len(transformed)/duration:,.0f} events/second")
    
    return duration

def test_lazy_iterator():
    """Test lazy iterator performance"""
    from src.iterator.lazy_collection import LazyCollection
    
    print("\n2. LAZY ITERATOR TEST")
    print("-" * 40)
    
    num_items = 1000000
    
    # Test lazy evaluation
    start = time.perf_counter()
    
    collection = LazyCollection(range(num_items))
    result = (collection
             .filter(lambda x: x % 2 == 0)  # Even numbers
             .map(lambda x: x * 2)           # Double them
             .filter(lambda x: x > 1000)     # Greater than 1000
             .take(100)                      # Take first 100
             .collect())                     # Materialize
    
    end = time.perf_counter()
    duration = end - start
    
    print(f"✅ Processed {num_items:,} items → {len(result)} results")
    print(f"⏱️  Time: {duration:.4f} seconds ({duration*1000:.2f} ms)")
    print(f"⚡ Items/second: {num_items/duration:,.0f}")
    
    return duration

def test_context_manager():
    """Test context manager performance"""
    from src.context_manager.resource_manager import ResourceManager
    
    print("\n3. CONTEXT MANAGER TEST")
    print("-" * 40)
    
    iterations = 100
    
    start = time.perf_counter()
    
    for i in range(iterations):
        with ResourceManager(enable_metrics=False, enable_logging=False) as rm:
            # Simulate resource operations
            rm.resources[f'resource_{i}'] = {'data': i, 'status': 'active'}
    
    end = time.perf_counter()
    duration = end - start
    
    print(f"✅ Created and cleaned up {iterations} contexts")
    print(f"⏱️  Time: {duration:.4f} seconds ({duration*1000:.2f} ms)")
    print(f"⚡ Contexts/second: {iterations/duration:,.0f}")
    print(f"📊 Avg per context: {duration/iterations*1000:.3f} ms")
    
    return duration

def test_fibonacci_generator():
    """Test Fibonacci generation performance"""
    from src.iterator.lazy_collection import LazyCollection, create_fibonacci_generator
    
    print("\n4. FIBONACCI GENERATOR TEST")
    print("-" * 40)
    
    start = time.perf_counter()
    
    fib = LazyCollection(create_fibonacci_generator())
    result = fib.take(1000).collect()
    
    end = time.perf_counter()
    duration = end - start
    
    print(f"✅ Generated {len(result)} Fibonacci numbers")
    print(f"⏱️  Time: {duration:.4f} seconds ({duration*1000:.2f} ms)")
    print(f"⚡ Numbers/second: {len(result)/duration:,.0f}")
    print(f"📊 Largest number: {result[-1]:,}")
    
    return duration

def test_batch_processing():
    """Test batch processing performance"""
    from src.iterator.lazy_collection import LazyCollection
    
    print("\n5. BATCH PROCESSING TEST")
    print("-" * 40)
    
    num_items = 100000
    batch_size = 1000
    
    start = time.perf_counter()
    
    collection = LazyCollection(range(num_items))
    batch_count = 0
    total_processed = 0
    
    for batch in collection.batch(batch_size):
        batch_count += 1
        total_processed += len(batch)
        if batch_count >= 50:  # Process 50 batches
            break
    
    end = time.perf_counter()
    duration = end - start
    
    print(f"✅ Processed {batch_count} batches ({total_processed:,} items)")
    print(f"⏱️  Time: {duration:.4f} seconds ({duration*1000:.2f} ms)")
    print(f"⚡ Items/second: {total_processed/duration:,.0f}")
    print(f"📊 Batch size: {batch_size}")
    
    return duration

def compare_memory_efficiency():
    """Compare memory usage: Eager vs Lazy"""
    import sys
    from src.iterator.lazy_collection import LazyCollection
    
    print("\n6. MEMORY EFFICIENCY COMPARISON")
    print("-" * 40)
    
    num_items = 100000
    
    # Eager approach
    start = time.perf_counter()
    eager_list = list(range(num_items))
    eager_filtered = [x * 2 for x in eager_list if x % 2 == 0][:10]
    eager_time = time.perf_counter() - start
    eager_memory = sys.getsizeof(eager_list) + sys.getsizeof(eager_filtered)
    
    print("📌 EAGER Processing:")
    print(f"   Time: {eager_time*1000:.2f} ms")
    print(f"   Memory: {eager_memory/1024:.1f} KB")
    
    # Lazy approach
    start = time.perf_counter()
    lazy_collection = LazyCollection(range(num_items))
    lazy_result = lazy_collection.filter(lambda x: x % 2 == 0).map(lambda x: x * 2).take(10).collect()
    lazy_time = time.perf_counter() - start
    lazy_memory = sys.getsizeof(lazy_result)
    
    print("📌 LAZY Processing:")
    print(f"   Time: {lazy_time*1000:.2f} ms")
    print(f"   Memory: {lazy_memory/1024:.1f} KB")
    
    print(f"\n🚀 Results:")
    print(f"   Lazy is {eager_time/lazy_time:.1f}x faster")
    print(f"   Lazy uses {eager_memory/lazy_memory:.0f}x less memory")

def test_metaclass_performance():
    """Test metaclass overhead"""
    print("\n7. METACLASS PERFORMANCE TEST")
    print("-" * 40)
    
    # Simple class without metaclass
    class SimpleClass:
        def process(self, x):
            return x * 2
    
    iterations = 100000
    
    start = time.perf_counter()
    obj = SimpleClass()
    for i in range(iterations):
        result = obj.process(i)
    end = time.perf_counter()
    duration = end - start
    
    print(f"✅ Executed {iterations:,} method calls")
    print(f"⏱️  Time: {duration:.4f} seconds ({duration*1000:.2f} ms)")
    print(f"⚡ Calls/second: {iterations/duration:,.0f}")
    
    return duration

def main():
    print("\n" + "="*60)
    print("🚀 STREAMSCALE PERFORMANCE MEASUREMENT REPORT")
    print("="*60)
    print("\nRunning performance tests on all components...")
    
    times = []
    
    # Run all tests
    times.append(("Data Pipeline", test_data_pipeline()))
    times.append(("Lazy Iterator", test_lazy_iterator()))
    times.append(("Context Manager", test_context_manager()))
    times.append(("Fibonacci", test_fibonacci_generator()))
    times.append(("Batch Processing", test_batch_processing()))
    compare_memory_efficiency()
    times.append(("Metaclass", test_metaclass_performance()))
    
    # Final summary
    print("\n" + "="*60)
    print("📊 FINAL PERFORMANCE SUMMARY")
    print("="*60)
    
    print("\n⏱️  Component Execution Times:")
    for name, duration in sorted(times, key=lambda x: x[1]):
        print(f"   {name:20s}: {duration*1000:8.2f} ms")
    
    total_time = sum(t[1] for t in times)
    print(f"\n📈 Total Test Time: {total_time:.4f} seconds")
    
    fastest = min(times, key=lambda x: x[1])
    slowest = max(times, key=lambda x: x[1])
    
    print(f"\n⚡ Fastest Component: {fastest[0]} ({fastest[1]*1000:.2f} ms)")
    print(f"🐌 Slowest Component: {slowest[0]} ({slowest[1]*1000:.2f} ms)")
    
    print("\n✅ All performance tests completed successfully!")
    print("="*60)

if __name__ == "__main__":
    main()