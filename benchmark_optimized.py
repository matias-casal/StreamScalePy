#!/usr/bin/env python3
"""
Benchmark comparing original vs optimized implementations.
Benchmark comparando implementaciones originales vs optimizadas.
"""
import time
import sys
import os
import gc

# Disable all logging
import logging
logging.disable(logging.CRITICAL)
os.environ['PYTHONWARNINGS'] = 'ignore'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipeline.models import DataEvent
from src.pipeline.processor_optimized import OptimizedDataProcessor, FastDataEvent, VectorizedProcessor
from src.iterator.lazy_collection import LazyCollection
from datetime import datetime


def benchmark_data_pipeline():
    """Compare original vs optimized data pipeline"""
    print("\n" + "="*60)
    print("📊 DATA PIPELINE BENCHMARK")
    print("="*60)
    
    num_events = 10000
    
    # Generate test data
    test_data = [
        {
            "event_id": f"evt_{i}",
            "source": f"sensor_{i % 10}",
            "event_type": "measurement",
            "data": {"value": i, "temperature": 20 + (i % 10), "humidity": 50 + (i % 20)}
        }
        for i in range(num_events)
    ]
    
    # Test 1: Original implementation with Pydantic
    print("\n⏱️  Original Implementation (Pydantic):")
    gc.collect()
    start = time.perf_counter()
    
    events_original = []
    for i in range(1000):
        event = DataEvent(
            event_id=f"evt_{i}",
            source="sensor_1",
            event_type="measurement",
            data={"value": i, "temperature": 20 + (i % 10)},
            timestamp=datetime.utcnow()
        )
        events_original.append(event)
    
    original_time = time.perf_counter() - start
    print(f"   Time: {original_time*1000:.2f} ms")
    print(f"   Throughput: {len(events_original)/original_time:,.0f} events/sec")
    
    # Test 2: Optimized implementation
    print("\n⚡ Optimized Implementation:")
    processor = OptimizedDataProcessor(batch_size=1000)
    gc.collect()
    start = time.perf_counter()
    
    events_optimized = []
    for data in test_data[:1000]:
        event = processor.transform_event_fast(data)
        if event:
            events_optimized.append(event)
    
    optimized_time = time.perf_counter() - start
    print(f"   Time: {optimized_time*1000:.2f} ms")
    print(f"   Throughput: {len(events_optimized)/optimized_time:,.0f} events/sec")
    
    # Test 3: Batch processing
    print("\n🚀 Batch Processing:")
    gc.collect()
    start = time.perf_counter()
    
    batch_count = 0
    for events, aggregations in processor.process_stream_batch(iter(test_data), batch_size=1000):
        batch_count += len(events)
        if batch_count >= 5000:
            break
    
    batch_time = time.perf_counter() - start
    print(f"   Time: {batch_time*1000:.2f} ms")
    print(f"   Throughput: {batch_count/batch_time:,.0f} events/sec")
    
    # Calculate improvements
    improvement_1 = original_time / optimized_time
    improvement_2 = original_time / batch_time
    
    print(f"\n📈 Improvements:")
    print(f"   Optimized is {improvement_1:.1f}x faster than original")
    print(f"   Batch processing is {improvement_2:.1f}x faster than original")
    
    return original_time, optimized_time, batch_time


def benchmark_lazy_iterator():
    """Compare different iterator optimizations"""
    print("\n" + "="*60)
    print("🔄 LAZY ITERATOR BENCHMARK")
    print("="*60)
    
    num_items = 1000000
    
    # Test 1: Original lazy iterator
    print("\n⏱️  Original Lazy Iterator:")
    gc.collect()
    start = time.perf_counter()
    
    collection = LazyCollection(range(num_items))
    result = (collection
             .filter(lambda x: x % 2 == 0)
             .map(lambda x: x * 2)
             .take(1000)
             .collect())
    
    original_time = time.perf_counter() - start
    print(f"   Time: {original_time*1000:.2f} ms")
    print(f"   Result count: {len(result)}")
    
    # Test 2: With generator expressions (more pythonic)
    print("\n⚡ Generator Expression:")
    gc.collect()
    start = time.perf_counter()
    
    gen = (x * 2 for x in range(num_items) if x % 2 == 0)
    result_gen = list(next(gen) for _ in range(1000))
    
    gen_time = time.perf_counter() - start
    print(f"   Time: {gen_time*1000:.2f} ms")
    print(f"   Result count: {len(result_gen)}")
    
    # Test 3: Using itertools for optimization
    print("\n🚀 Itertools Optimization:")
    from itertools import islice, compress
    gc.collect()
    start = time.perf_counter()
    
    # Create filter mask
    mask = (i % 2 == 0 for i in range(num_items))
    filtered = compress(range(num_items), mask)
    mapped = (x * 2 for x in filtered)
    result_itertools = list(islice(mapped, 1000))
    
    itertools_time = time.perf_counter() - start
    print(f"   Time: {itertools_time*1000:.2f} ms")
    print(f"   Result count: {len(result_itertools)}")
    
    # Calculate improvements
    improvement_1 = original_time / gen_time
    improvement_2 = original_time / itertools_time
    
    print(f"\n📈 Improvements:")
    print(f"   Generator expression is {improvement_1:.1f}x faster")
    print(f"   Itertools is {improvement_2:.1f}x faster")
    
    return original_time, gen_time, itertools_time


def benchmark_vectorized_operations():
    """Benchmark vectorized operations if numpy available"""
    print("\n" + "="*60)
    print("🎯 VECTORIZED OPERATIONS BENCHMARK")
    print("="*60)
    
    values = [float(i) for i in range(100000)]
    
    # Test 1: Regular Python operations
    print("\n⏱️  Regular Python:")
    gc.collect()
    start = time.perf_counter()
    
    results_regular = {
        'min': min(values),
        'max': max(values),
        'avg': sum(values) / len(values),
        'sum': sum(values)
    }
    
    regular_time = time.perf_counter() - start
    print(f"   Time: {regular_time*1000:.2f} ms")
    
    # Test 2: Try vectorized with numpy
    try:
        import numpy as np
        print("\n⚡ NumPy Vectorized:")
        gc.collect()
        start = time.perf_counter()
        
        arr = np.array(values)
        results_numpy = {
            'min': np.min(arr),
            'max': np.max(arr),
            'avg': np.mean(arr),
            'sum': np.sum(arr),
            'std': np.std(arr),
            'median': np.median(arr)
        }
        
        numpy_time = time.perf_counter() - start
        print(f"   Time: {numpy_time*1000:.2f} ms")
        print(f"   Extra metrics: std={results_numpy['std']:.2f}, median={results_numpy['median']:.2f}")
        
        improvement = regular_time / numpy_time
        print(f"\n📈 NumPy is {improvement:.1f}x faster")
        
        return regular_time, numpy_time
        
    except ImportError:
        print("\n⚠️  NumPy not installed - skipping vectorized test")
        print("   Install with: pip install numpy")
        return regular_time, None


def benchmark_caching():
    """Benchmark caching effectiveness"""
    print("\n" + "="*60)
    print("💾 CACHING BENCHMARK")
    print("="*60)
    
    from functools import lru_cache
    
    # Expensive function without cache
    def expensive_transform(value):
        # Simulate expensive operation
        result = value
        for _ in range(100):
            result = (result * 7 + 13) % 1000000
        return result
    
    # Same function with cache
    @lru_cache(maxsize=1000)
    def expensive_transform_cached(value):
        result = value
        for _ in range(100):
            result = (result * 7 + 13) % 1000000
        return result
    
    # Test data with repetitions
    test_values = [i % 100 for i in range(10000)]  # Many repeated values
    
    # Test 1: Without cache
    print("\n⏱️  Without Cache:")
    gc.collect()
    start = time.perf_counter()
    
    results_no_cache = [expensive_transform(v) for v in test_values]
    
    no_cache_time = time.perf_counter() - start
    print(f"   Time: {no_cache_time*1000:.2f} ms")
    
    # Test 2: With cache
    print("\n⚡ With LRU Cache:")
    expensive_transform_cached.cache_clear()  # Clear cache
    gc.collect()
    start = time.perf_counter()
    
    results_cached = [expensive_transform_cached(v) for v in test_values]
    
    cache_time = time.perf_counter() - start
    print(f"   Time: {cache_time*1000:.2f} ms")
    
    cache_info = expensive_transform_cached.cache_info()
    print(f"   Cache hits: {cache_info.hits:,}")
    print(f"   Cache misses: {cache_info.misses}")
    print(f"   Hit rate: {cache_info.hits/(cache_info.hits + cache_info.misses)*100:.1f}%")
    
    improvement = no_cache_time / cache_time
    print(f"\n📈 Caching is {improvement:.1f}x faster for repeated operations")
    
    return no_cache_time, cache_time


def main():
    print("\n" + "="*80)
    print("🚀 STREAMSCALE OPTIMIZATION BENCHMARK")
    print("="*80)
    print("\nComparing original vs optimized implementations...")
    
    all_results = []
    
    # Run benchmarks
    pipeline_results = benchmark_data_pipeline()
    iterator_results = benchmark_lazy_iterator()
    vector_results = benchmark_vectorized_operations()
    cache_results = benchmark_caching()
    
    # Final summary
    print("\n" + "="*80)
    print("📊 OPTIMIZATION SUMMARY")
    print("="*80)
    
    print("\n🏆 Best Improvements:")
    
    if pipeline_results:
        original, optimized, batch = pipeline_results
        print(f"\n📌 Data Pipeline:")
        print(f"   Original: {original*1000:.2f} ms")
        print(f"   Optimized: {optimized*1000:.2f} ms ({original/optimized:.1f}x faster)")
        print(f"   Batch: {batch*1000:.2f} ms ({original/batch:.1f}x faster)")
    
    if iterator_results:
        original, gen, itertools = iterator_results
        print(f"\n📌 Lazy Iterator:")
        print(f"   Original: {original*1000:.2f} ms")
        print(f"   Best: {min(gen, itertools)*1000:.2f} ms ({original/min(gen, itertools):.1f}x faster)")
    
    if vector_results[1] is not None:
        regular, numpy = vector_results
        print(f"\n📌 Vectorized Ops:")
        print(f"   Regular: {regular*1000:.2f} ms")
        print(f"   NumPy: {numpy*1000:.2f} ms ({regular/numpy:.1f}x faster)")
    
    if cache_results:
        no_cache, cached = cache_results
        print(f"\n📌 Caching:")
        print(f"   No cache: {no_cache*1000:.2f} ms")
        print(f"   With cache: {cached*1000:.2f} ms ({no_cache/cached:.1f}x faster)")
    
    print("\n✅ Optimization benchmark complete!")
    print("="*80)


if __name__ == "__main__":
    main()