"""
Custom iterator with lazy evaluation for memory-efficient processing.
Iterador personalizado con evaluación perezosa para procesamiento eficiente en memoria.
"""
from typing import (
    Iterator, Callable, Any, Optional, List, TypeVar, 
    Generic, Union, Tuple, Dict, Generator
)
from functools import reduce, wraps
from itertools import islice, chain
import asyncio
from dataclasses import dataclass
from collections import deque
import psycopg2
from contextlib import contextmanager

from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')
U = TypeVar('U')


@dataclass
class Transform:
    """
    Represents a transformation to be applied lazily.
    Representa una transformación a aplicar perezosamente.
    """
    operation: str  # 'map', 'filter', 'flat_map', etc.
    func: Callable
    args: Tuple = ()
    kwargs: Dict = None
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}


class LazyCollection(Generic[T]):
    """
    Custom collection class with lazy evaluation.
    Clase de colección personalizada con evaluación perezosa.
    
    Memory usage scales with output size, not input size.
    El uso de memoria escala con el tamaño de salida, no con el tamaño de entrada.
    """
    
    def __init__(self, 
                 source: Union[Iterator[T], List[T], Generator[T, None, None], Callable[[], Iterator[T]]],
                 transforms: Optional[List[Transform]] = None):
        """
        Initialize lazy collection.
        
        Args:
            source: Data source (iterator, list, generator, or callable returning iterator)
            transforms: List of transformations to apply
        """
        self._source = source
        self._transforms = transforms or []
        self._materialized = False
        self._cache = None
        
    def __iter__(self) -> Iterator[T]:
        """
        Create iterator that applies all transformations lazily.
        Crea iterador que aplica todas las transformaciones perezosamente.
        """
        # Get source iterator
        if callable(self._source):
            iterator = self._source()
        elif hasattr(self._source, '__iter__'):
            # Handle any iterable including range, list, etc.
            iterator = iter(self._source)
        else:
            # Assume it's already an iterator
            iterator = self._source
        
        # Apply transformations in order
        for transform in self._transforms:
            iterator = self._apply_transform(iterator, transform)
        
        return iterator
    
    def _apply_transform(self, iterator: Iterator, transform: Transform) -> Iterator:
        """
        Apply a single transformation to an iterator.
        Aplica una única transformación a un iterador.
        """
        if transform.operation == 'map':
            return map(transform.func, iterator)
        
        elif transform.operation == 'filter':
            return filter(transform.func, iterator)
        
        elif transform.operation == 'flat_map':
            return chain.from_iterable(map(transform.func, iterator))
        
        elif transform.operation == 'take':
            limit = transform.args[0] if transform.args else 10
            return islice(iterator, limit)
        
        elif transform.operation == 'skip':
            n = transform.args[0] if transform.args else 0
            return islice(iterator, n, None)
        
        elif transform.operation == 'unique':
            return self._unique_iterator(iterator, transform.func)
        
        elif transform.operation == 'window':
            size = transform.args[0] if transform.args else 2
            return self._window_iterator(iterator, size)
        
        elif transform.operation == 'batch':
            size = transform.args[0] if transform.args else 10
            return self._batch_iterator(iterator, size)
        
        else:
            raise ValueError(f"Unknown operation: {transform.operation}")
    
    def _unique_iterator(self, iterator: Iterator, key_func: Optional[Callable] = None) -> Iterator:
        """
        Iterator that yields unique elements.
        Iterador que produce elementos únicos.
        """
        seen = set()
        for item in iterator:
            key = key_func(item) if key_func else item
            if key not in seen:
                seen.add(key)
                yield item
    
    def _window_iterator(self, iterator: Iterator, size: int) -> Iterator[List]:
        """
        Sliding window iterator.
        Iterador de ventana deslizante.
        """
        window = deque(maxlen=size)
        for item in iterator:
            window.append(item)
            if len(window) == size:
                yield list(window)
    
    def _batch_iterator(self, iterator: Iterator, batch_size: int) -> Iterator[List]:
        """
        Batch iterator that groups elements.
        Iterador por lotes que agrupa elementos.
        """
        batch = []
        for item in iterator:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
    
    # Transformation methods (return new LazyCollection)
    
    def map(self, func: Callable[[T], U]) -> 'LazyCollection[U]':
        """
        Apply a mapping function lazily.
        Aplica una función de mapeo perezosamente.
        """
        new_transforms = self._transforms + [Transform('map', func)]
        return LazyCollection(self._source, new_transforms)
    
    def filter(self, predicate: Callable[[T], bool]) -> 'LazyCollection[T]':
        """
        Filter elements lazily.
        Filtra elementos perezosamente.
        """
        new_transforms = self._transforms + [Transform('filter', predicate)]
        return LazyCollection(self._source, new_transforms)
    
    def flat_map(self, func: Callable[[T], Iterator[U]]) -> 'LazyCollection[U]':
        """
        Flat map operation (map then flatten).
        Operación flat map (mapear luego aplanar).
        """
        new_transforms = self._transforms + [Transform('flat_map', func)]
        return LazyCollection(self._source, new_transforms)
    
    def take(self, n: int) -> 'LazyCollection[T]':
        """
        Take first n elements.
        Toma los primeros n elementos.
        """
        new_transforms = self._transforms + [Transform('take', lambda x: x, args=(n,))]
        return LazyCollection(self._source, new_transforms)
    
    def skip(self, n: int) -> 'LazyCollection[T]':
        """
        Skip first n elements.
        Omite los primeros n elementos.
        """
        new_transforms = self._transforms + [Transform('skip', lambda x: x, args=(n,))]
        return LazyCollection(self._source, new_transforms)
    
    def unique(self, key: Optional[Callable[[T], Any]] = None) -> 'LazyCollection[T]':
        """
        Keep only unique elements.
        Mantiene solo elementos únicos.
        """
        new_transforms = self._transforms + [Transform('unique', key)]
        return LazyCollection(self._source, new_transforms)
    
    def window(self, size: int) -> 'LazyCollection[List[T]]':
        """
        Create sliding windows of elements.
        Crea ventanas deslizantes de elementos.
        """
        new_transforms = self._transforms + [Transform('window', lambda x: x, args=(size,))]
        return LazyCollection(self._source, new_transforms)
    
    def batch(self, batch_size: int) -> 'LazyCollection[List[T]]':
        """
        Batch elements into groups.
        Agrupa elementos en lotes.
        """
        new_transforms = self._transforms + [Transform('batch', lambda x: x, args=(batch_size,))]
        return LazyCollection(self._source, new_transforms)
    
    # Terminal operations (force evaluation)
    
    def reduce(self, func: Callable[[U, T], U], initial: U) -> U:
        """
        Reduce collection to a single value.
        Reduce la colección a un único valor.
        """
        return reduce(func, self, initial)
    
    def collect(self) -> List[T]:
        """
        Materialize the collection into a list.
        Materializa la colección en una lista.
        """
        if self._cache is not None:
            return self._cache
        
        self._cache = list(self)
        self._materialized = True
        return self._cache
    
    def first(self) -> Optional[T]:
        """
        Get the first element.
        Obtiene el primer elemento.
        """
        try:
            return next(iter(self))
        except StopIteration:
            return None
    
    def count(self) -> int:
        """
        Count elements (forces evaluation).
        Cuenta elementos (fuerza evaluación).
        """
        return sum(1 for _ in self)
    
    def foreach(self, func: Callable[[T], None]) -> None:
        """
        Apply a function to each element (side effects).
        Aplica una función a cada elemento (efectos secundarios).
        """
        for item in self:
            func(item)
    
    def partition(self, predicate: Callable[[T], bool]) -> Tuple['LazyCollection[T]', 'LazyCollection[T]']:
        """
        Partition into two collections based on predicate.
        Particiona en dos colecciones basado en predicado.
        """
        # This requires materialization to avoid consuming iterator twice
        items = self.collect()
        true_items = [x for x in items if predicate(x)]
        false_items = [x for x in items if not predicate(x)]
        return LazyCollection(true_items), LazyCollection(false_items)
    
    def paginate(self, page_size: int) -> Iterator['LazyCollection[T]']:
        """
        Create paginated collections.
        Crea colecciones paginadas.
        """
        iterator = iter(self)
        while True:
            page_items = list(islice(iterator, page_size))
            if not page_items:
                break
            yield LazyCollection(page_items)
    
    def chunk(self, chunk_size: int) -> 'LazyCollection[List[T]]':
        """
        Alias for batch operation.
        Alias para operación batch.
        """
        return self.batch(chunk_size)
    
    # Advanced operations
    
    def cache(self) -> 'LazyCollection[T]':
        """
        Cache results after first evaluation.
        Cachea resultados después de la primera evaluación.
        """
        if self._cache is None:
            self._cache = list(self)
        return LazyCollection(self._cache)
    
    def parallel_map(self, func: Callable[[T], U], max_workers: int = 4) -> 'LazyCollection[U]':
        """
        Map with parallel execution (for CPU-bound operations).
        Mapeo con ejecución paralela (para operaciones CPU-intensivas).
        """
        from concurrent.futures import ThreadPoolExecutor
        
        def parallel_generator():
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                futures = []
                items = []
                for item in self:
                    items.append(item)
                    futures.append(executor.submit(func, item))
                
                # Yield results as they complete
                for future in futures:
                    yield future.result()
        
        return LazyCollection(parallel_generator)
    
    def peek(self, func: Callable[[T], None]) -> 'LazyCollection[T]':
        """
        Peek at elements without consuming (for debugging).
        Observa elementos sin consumir (para depuración).
        """
        def peek_generator():
            for item in self:
                func(item)
                yield item
        
        return LazyCollection(peek_generator)
    
    def __repr__(self) -> str:
        """String representation / Representación en cadena"""
        transform_names = [t.operation for t in self._transforms]
        return f"LazyCollection(transforms={transform_names}, materialized={self._materialized})"


class DatabaseLazyCollection(LazyCollection[Dict[str, Any]]):
    """
    Lazy collection that fetches data from PostgreSQL.
    Colección perezosa que obtiene datos de PostgreSQL.
    """
    
    def __init__(self, 
                 query: str,
                 params: Optional[Tuple] = None,
                 fetch_size: int = 1000,
                 connection_params: Optional[Dict] = None):
        """
        Initialize database lazy collection.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            fetch_size: Number of rows to fetch at a time
            connection_params: Database connection parameters
        """
        self.query = query
        self.params = params or ()
        self.fetch_size = fetch_size
        self.connection_params = connection_params or {
            'host': settings.database.host,
            'port': settings.database.port,
            'database': settings.database.database,
            'user': settings.database.user,
            'password': settings.database.password
        }
        
        # Initialize with a generator function
        super().__init__(self._create_generator)
    
    @contextmanager
    def _get_connection(self):
        """Get database connection context / Obtiene contexto de conexión a base de datos"""
        conn = psycopg2.connect(**self.connection_params)
        try:
            yield conn
        finally:
            conn.close()
    
    def _create_generator(self) -> Generator[Dict[str, Any], None, None]:
        """
        Create generator that fetches from database.
        Crea generador que obtiene de la base de datos.
        """
        with self._get_connection() as conn:
            with conn.cursor(name='lazy_cursor') as cursor:
                # Use server-side cursor for memory efficiency
                cursor.itersize = self.fetch_size
                cursor.execute(self.query, self.params)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Yield rows as dictionaries
                for row in cursor:
                    yield dict(zip(columns, row))
    
    @classmethod
    def from_table(cls, 
                  table_name: str,
                  columns: Optional[List[str]] = None,
                  where: Optional[str] = None,
                  order_by: Optional[str] = None,
                  limit: Optional[int] = None,
                  **kwargs) -> 'DatabaseLazyCollection':
        """
        Create collection from table with optional filters.
        Crea colección desde tabla con filtros opcionales.
        """
        # Build query
        select_clause = ", ".join(columns) if columns else "*"
        query = f"SELECT {select_clause} FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"
        
        return cls(query, **kwargs)


def create_fibonacci_generator(limit: int = None) -> Generator[int, None, None]:
    """
    Create Fibonacci number generator for testing.
    Crea generador de números Fibonacci para pruebas.
    """
    a, b = 0, 1
    count = 0
    while limit is None or count < limit:
        yield a
        a, b = b, a + b
        count += 1


def example_usage():
    """
    Example usage of LazyCollection.
    Ejemplo de uso de LazyCollection.
    """
    print("LazyCollection Examples:")
    
    # Example 1: Basic transformations
    numbers = LazyCollection(range(1, 11))
    result = (numbers
              .map(lambda x: x * 2)
              .filter(lambda x: x > 5)
              .map(lambda x: x ** 2)
              .take(3)
              .collect())
    print(f"1. Transformed numbers: {result}")
    
    # Example 2: Window operations
    data = LazyCollection(range(1, 8))
    windows = data.window(3).collect()
    print(f"2. Sliding windows: {windows}")
    
    # Example 3: Batch processing
    items = LazyCollection(range(1, 16))
    batches = items.batch(5).collect()
    print(f"3. Batches: {batches}")
    
    # Example 4: Fibonacci with lazy evaluation
    fib = LazyCollection(create_fibonacci_generator())
    fib_result = (fib
                  .take(10)
                  .filter(lambda x: x % 2 == 0)
                  .collect())
    print(f"4. Even Fibonacci numbers: {fib_result}")
    
    # Example 5: Chaining and pagination
    large_data = LazyCollection(range(1, 101))
    pages = list(large_data
                 .filter(lambda x: x % 2 == 0)
                 .map(lambda x: x * 3)
                 .paginate(10))
    print(f"5. Number of pages: {len(pages)}")
    print(f"   First page: {pages[0].collect()}")
    
    # Example 6: Parallel processing
    compute_intensive = LazyCollection(range(1, 6))
    parallel_result = (compute_intensive
                      .parallel_map(lambda x: x ** 3, max_workers=2)
                      .collect())
    print(f"6. Parallel computation: {parallel_result}")
    
    print("\nAll examples completed successfully!")


if __name__ == "__main__":
    example_usage()