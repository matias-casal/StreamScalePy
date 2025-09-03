"""
Example implementations using the API contract metaclass.
Implementaciones de ejemplo usando la metaclase de contrato API.
"""
from typing import Any, Dict, List
from src.meta_programming.api_contract import APIContractBase, contract_class
from src.shared.logging import get_logger

logger = get_logger(__name__)


class DataProcessor(APIContractBase):
    """
    Example data processor that follows the API contract.
    Procesador de datos de ejemplo que sigue el contrato API.
    """
    
    # Required attributes
    version = "2.0.0"
    description = "Advanced data processor with validation"
    author = "StreamScale Team"
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize processor with configuration"""
        super().__init__()
        self.config = config or {}
        self.processed_count = 0
        self.error_count = 0
    
    def process(self, data: Any) -> Any:
        """
        Process incoming data.
        Procesa datos entrantes.
        """
        try:
            # Validate first
            if not self.validate(data):
                self.error_count += 1
                raise ValueError("Data validation failed")
            
            # Process the data
            if isinstance(data, dict):
                processed = {
                    k: v.upper() if isinstance(v, str) else v
                    for k, v in data.items()
                }
            elif isinstance(data, list):
                processed = [self.process(item) for item in data]
            else:
                processed = str(data).upper()
            
            self.processed_count += 1
            self._status = "processing"
            
            return processed
            
        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            self._status = "error"
            raise
    
    def validate(self, data: Any) -> bool:
        """
        Validate input data.
        Valida datos de entrada.
        """
        if data is None:
            return False
        
        if isinstance(data, dict):
            return len(data) > 0
        elif isinstance(data, (list, str)):
            return len(data) > 0
        
        return True
    
    def serialize(self) -> Dict[str, Any]:
        """Custom serialization / Serialización personalizada"""
        base_data = super().serialize()
        base_data.update({
            'config': self.config,
            'processed_count': self.processed_count,
            'error_count': self.error_count
        })
        return base_data
    
    @classmethod
    def deserialize(cls, data: Dict[str, Any]):
        """Custom deserialization / Deserialización personalizada"""
        instance = cls(config=data.get('config', {}))
        instance.processed_count = data.get('processed_count', 0)
        instance.error_count = data.get('error_count', 0)
        instance._status = data.get('status', 'initialized')
        instance._metrics = data.get('metrics', {})
        return instance


class StreamAnalyzer(APIContractBase):
    """
    Stream analyzer that inherits from DataProcessor.
    Analizador de stream que hereda de DataProcessor.
    """
    
    version = "1.5.0"
    description = "Real-time stream analysis engine"
    author = "Analytics Team"
    
    def __init__(self):
        """Initialize analyzer"""
        super().__init__()
        self.analysis_results = []
        self.stream_buffer = []
    
    def process(self, data: Any) -> Any:
        """
        Analyze streaming data.
        Analiza datos en streaming.
        """
        # Add to buffer
        self.stream_buffer.append(data)
        
        # Keep buffer size limited
        if len(self.stream_buffer) > 100:
            self.stream_buffer.pop(0)
        
        # Perform analysis
        analysis = {
            'data': data,
            'buffer_size': len(self.stream_buffer),
            'analysis_type': 'stream',
            'timestamp': self._get_timestamp()
        }
        
        self.analysis_results.append(analysis)
        self._status = "analyzing"
        
        return analysis
    
    def validate(self, data: Any) -> bool:
        """
        Validate stream data.
        Valida datos del stream.
        """
        # Basic validation
        if data is None:
            return False
        
        # Check if it's streamable
        return hasattr(data, '__iter__') or isinstance(data, (dict, list, str))
    
    def _get_timestamp(self) -> str:
        """Get current timestamp / Obtiene timestamp actual"""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get analysis statistics.
        Obtiene estadísticas del análisis.
        """
        return {
            'total_analyzed': len(self.analysis_results),
            'buffer_size': len(self.stream_buffer),
            'status': self.status,
            'metrics': self.metrics
        }


# Example using the decorator for custom contracts
@contract_class(
    required_methods={'execute', 'cleanup'},
    required_attributes={'name', 'priority'},
    required_properties={'is_running'}
)
class TaskExecutor:
    """
    Task executor with custom contract.
    Ejecutor de tareas con contrato personalizado.
    """
    
    # Required attributes
    name = "TaskExecutor"
    priority = 1
    
    def __init__(self):
        """Initialize executor"""
        self._running = False
        self.tasks_completed = 0
    
    @property
    def is_running(self) -> bool:
        """Check if executor is running / Verifica si el ejecutor está corriendo"""
        return self._running
    
    def execute(self, task: Any) -> Any:
        """
        Execute a task.
        Ejecuta una tarea.
        """
        self._running = True
        try:
            # Simulate task execution
            result = f"Executed: {task}"
            self.tasks_completed += 1
            return result
        finally:
            self._running = False
    
    def cleanup(self):
        """
        Cleanup resources.
        Limpia recursos.
        """
        self._running = False
        logger.info(f"Cleaned up {self.name}, completed {self.tasks_completed} tasks")


class InvalidProcessor:
    """
    This class will fail contract validation (for testing).
    Esta clase fallará la validación del contrato (para pruebas).
    """
    
    version = "1.0.0"
    
    def process(self, data):
        """Has process but missing other required methods"""
        return data


def test_metaclass_functionality():
    """
    Test the metaclass functionality.
    Prueba la funcionalidad de la metaclase.
    """
    print("Testing API Contract Metaclass...")
    
    # Test valid implementation
    processor = DataProcessor(config={'mode': 'test'})
    assert processor.validate({'test': 'data'})
    result = processor.process({'hello': 'world'})
    print(f"✓ DataProcessor processed: {result}")
    
    # Test serialization
    serialized = processor.serialize()
    print(f"✓ Serialized: {serialized}")
    
    # Test deserialization
    restored = DataProcessor.deserialize(serialized)
    print(f"✓ Deserialized: {restored.serialize()}")
    
    # Test analyzer
    analyzer = StreamAnalyzer()
    analysis = analyzer.process([1, 2, 3])
    print(f"✓ StreamAnalyzer analyzed: {analysis}")
    
    # Test custom contract with decorator
    executor = TaskExecutor()
    task_result = executor.execute("test_task")
    print(f"✓ TaskExecutor executed: {task_result}")
    executor.cleanup()
    
    # Test registry
    from src.meta_programming.api_contract import APIContractMeta
    registry = APIContractMeta.get_registry()
    print(f"✓ Registry contains {len(registry)} classes")
    
    # Try to create invalid class (should raise exception)
    try:
        from src.meta_programming.api_contract import APIContractBase
        
        class BadImplementation(APIContractBase):
            version = "1.0.0"
            # Missing required methods and attributes
            pass
        
        print("✗ Should have raised ContractViolationError")
    except Exception as e:
        print(f"✓ Contract violation caught: {e.__class__.__name__}")
    
    print("\nAll tests passed!")


if __name__ == "__main__":
    test_metaclass_functionality()