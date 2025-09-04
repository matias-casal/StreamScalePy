"""
Advanced meta-programming with API contract enforcement.
Meta-programación avanzada con cumplimiento de contrato API.
"""
import inspect
import redis
import json
from typing import Dict, Any, List, Type, Callable, Optional, Set, Tuple
from functools import wraps
from datetime import datetime
import hashlib

from src.shared.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


class ContractViolationError(Exception):
    """
    Exception raised when API contract is violated.
    Excepción lanzada cuando se viola el contrato API.
    """
    pass


class APIContractMeta(type):
    """
    Metaclass that enforces API contracts and auto-registers classes.
    Metaclase que aplica contratos API y registra clases automáticamente.
    """
    
    # Class registry stored in memory and Redis
    _registry: Dict[str, Type] = {}
    _redis_client: Optional[redis.Redis] = None
    _registry_key = "api_contract:registry"
    
    # Required methods and attributes for the contract
    _required_methods: Set[str] = {
        'process',
        'validate', 
        'serialize',
        'deserialize'
    }
    
    _required_attributes: Set[str] = {
        'version',
        'description',
        'author'
    }
    
    _required_properties: Set[str] = {
        'status',
        'metrics'
    }
    
    @classmethod
    def set_contract(cls,
                    required_methods: Optional[Set[str]] = None,
                    required_attributes: Optional[Set[str]] = None,
                    required_properties: Optional[Set[str]] = None):
        """
        Set the API contract requirements.
        Establece los requisitos del contrato API.
        """
        if required_methods:
            cls._required_methods = required_methods
        if required_attributes:
            cls._required_attributes = required_attributes
        if required_properties:
            cls._required_properties = required_properties
    
    @classmethod
    def get_redis_client(cls) -> Optional[redis.Redis]:
        """Get or create Redis client / Obtiene o crea cliente Redis"""
        if cls._redis_client is None:
            try:
                cls._redis_client = redis.Redis(
                    host=settings.redis.host,
                    port=settings.redis.port,
                    db=settings.redis.db,
                    decode_responses=True
                )
                # Test connection
                cls._redis_client.ping()
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning(f"Could not connect to Redis: {str(e)}")
                cls._redis_client = None
        return cls._redis_client
    
    def __new__(mcs, name: str, bases: Tuple[Type, ...], namespace: Dict[str, Any]):
        """
        Create new class and enforce contract.
        Crea nueva clase y aplica el contrato.
        """
        # Skip contract checking for the base class itself
        if name == 'APIContractBase':
            return super().__new__(mcs, name, bases, namespace)
        
        # Check if this is a concrete implementation (not abstract)
        is_abstract = namespace.get('__abstract__', False)
        
        if not is_abstract:
            # Validate contract for concrete classes
            mcs._validate_contract(name, bases, namespace)
            
            # Add validation decorators to methods
            namespace = mcs._add_validation_decorators(namespace)
            
            # Add automatic serialization methods if not present
            namespace = mcs._add_serialization_methods(namespace)
            
            # Add metrics collection
            namespace = mcs._add_metrics_collection(namespace)
        
        # Create the class
        cls = super().__new__(mcs, name, bases, namespace)
        
        # Register the class if it's not abstract
        if not is_abstract:
            mcs._register_class(cls)
            logger.info(f"Class '{name}' created and registered with API contract")
        
        return cls
    
    @classmethod
    def _validate_contract(mcs, name: str, bases: Tuple[Type, ...], namespace: Dict[str, Any]):
        """
        Validate that the class meets the API contract.
        Valida que la clase cumple con el contrato API.
        """
        # Collect all attributes and methods from bases
        inherited = set()
        for base in bases:
            inherited.update(dir(base))
        
        all_members = set(namespace.keys()) | inherited
        
        # Check required methods
        missing_methods = []
        for method_name in mcs._required_methods:
            if method_name not in all_members:
                missing_methods.append(method_name)
            elif method_name in namespace and not callable(namespace[method_name]):
                raise ContractViolationError(
                    f"Class '{name}' has '{method_name}' but it is not callable. "
                    f"Required methods: {mcs._required_methods}"
                )
        
        # Check required attributes
        missing_attributes = []
        for attr_name in mcs._required_attributes:
            if attr_name not in namespace:
                missing_attributes.append(attr_name)
        
        # Check required properties
        missing_properties = []
        for prop_name in mcs._required_properties:
            if prop_name not in all_members:
                missing_properties.append(prop_name)
            elif prop_name in namespace:
                prop = namespace[prop_name]
                if not isinstance(prop, property) and not hasattr(prop, 'fget'):
                    raise ContractViolationError(
                        f"Class '{name}' has '{prop_name}' but it is not a property. "
                        f"Required properties: {mcs._required_properties}"
                    )
        
        # Raise comprehensive error if contract is violated
        errors = []
        if missing_methods:
            errors.append(f"Missing required methods: {missing_methods}")
        if missing_attributes:
            errors.append(f"Missing required attributes: {missing_attributes}")
        if missing_properties:
            errors.append(f"Missing required properties: {missing_properties}")
        
        if errors:
            error_message = f"Class '{name}' violates API contract:\n" + "\n".join(errors)
            error_message += f"\n\nRequired contract:"
            error_message += f"\n  Methods: {mcs._required_methods}"
            error_message += f"\n  Attributes: {mcs._required_attributes}"
            error_message += f"\n  Properties: {mcs._required_properties}"
            raise ContractViolationError(error_message)
    
    @classmethod
    def _add_validation_decorators(mcs, namespace: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add runtime validation to methods.
        Agrega validación en tiempo de ejecución a los métodos.
        """
        # Methods that should not have validation decorators
        skip_validation = {'serialize', 'deserialize'}
        
        for name, value in namespace.items():
            if name in mcs._required_methods and callable(value) and name not in skip_validation:
                namespace[name] = mcs._validate_method(value)
        return namespace
    
    @classmethod
    def _validate_method(mcs, method: Callable) -> Callable:
        """
        Wrap method with validation logic.
        Envuelve el método con lógica de validación.
        """
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            # Log method call
            logger.debug(f"Calling {self.__class__.__name__}.{method.__name__}")
            
            # Validate inputs
            if hasattr(self, f'_validate_{method.__name__}_input'):
                validator = getattr(self, f'_validate_{method.__name__}_input')
                validator(*args, **kwargs)
            
            # Execute method
            result = method(self, *args, **kwargs)
            
            # Validate output
            if hasattr(self, f'_validate_{method.__name__}_output'):
                validator = getattr(self, f'_validate_{method.__name__}_output')
                validator(result)
            
            return result
        
        return wrapper
    
    @classmethod
    def _add_serialization_methods(mcs, namespace: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add default serialization methods if not present.
        Agrega métodos de serialización por defecto si no están presentes.
        """
        # Don't add if already inherited from base class
        has_serialize = 'serialize' in namespace or any(
            hasattr(base, 'serialize') for base in namespace.get('__bases__', [])
        )
        has_deserialize = 'deserialize' in namespace or any(
            hasattr(base, 'deserialize') for base in namespace.get('__bases__', [])
        )
        
        if not has_serialize:
            def serialize(self) -> Dict[str, Any]:
                """Default serialization / Serialización por defecto"""
                return {
                    'class': self.__class__.__name__,
                    'version': getattr(self, 'version', '1.0.0'),
                    'data': {k: v for k, v in self.__dict__.items() 
                            if not k.startswith('_')}
                }
            namespace['serialize'] = serialize
        
        if not has_deserialize:
            @classmethod
            def deserialize(cls, data: Dict[str, Any]):
                """Default deserialization / Deserialización por defecto"""
                instance = cls.__new__(cls)
                for key, value in data.get('data', {}).items():
                    setattr(instance, key, value)
                if hasattr(instance, '__init__'):
                    instance.__init__()
                return instance
            namespace['deserialize'] = deserialize
        
        return namespace
    
    @classmethod
    def _add_metrics_collection(mcs, namespace: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add metrics collection to methods.
        Agrega recolección de métricas a los métodos.
        """
        # Add metrics storage
        if '_metrics' not in namespace:
            namespace['_metrics'] = {}
        
        # Methods that should not have metrics collection
        skip_metrics = {'serialize', 'deserialize'}
        
        # Helper function to create metrics wrapper with proper closure
        def create_metrics_wrapper(method):
            @wraps(method)
            def metrics_wrapper(self, *args, **kwargs):
                import time
                start_time = time.time()
                
                try:
                    result = method(self, *args, **kwargs)
                    
                    # Record success metric
                    if not hasattr(self, '_metrics'):
                        self._metrics = {}
                    
                    method_name = method.__name__
                    if method_name not in self._metrics:
                        self._metrics[method_name] = {
                            'calls': 0,
                            'errors': 0,
                            'total_time': 0.0,
                            'last_call': None
                        }
                    
                    self._metrics[method_name]['calls'] += 1
                    self._metrics[method_name]['total_time'] += time.time() - start_time
                    self._metrics[method_name]['last_call'] = datetime.utcnow().isoformat()
                    
                    return result
                    
                except Exception as e:
                    # Record error metric
                    method_name = method.__name__
                    if hasattr(self, '_metrics') and method_name in self._metrics:
                        self._metrics[method_name]['errors'] += 1
                    raise
            return metrics_wrapper
        
        # Wrap methods to collect metrics
        for name, value in list(namespace.items()):
            if name in mcs._required_methods and callable(value) and name not in skip_metrics:
                namespace[name] = create_metrics_wrapper(value)
        
        return namespace
    
    @classmethod
    def _register_class(mcs, cls: Type):
        """
        Register class in registry.
        Registra la clase en el registro.
        """
        class_id = mcs._generate_class_id(cls)
        
        # Register in memory
        mcs._registry[class_id] = cls
        
        # Register in Redis for persistence if available
        try:
            redis_client = mcs.get_redis_client()
            if redis_client:
                # Prepare class metadata
                metadata = {
                    'class_name': cls.__name__,
                    'module': cls.__module__,
                    'version': getattr(cls, 'version', '1.0.0'),
                    'description': getattr(cls, 'description', ''),
                    'author': getattr(cls, 'author', ''),
                    'registered_at': datetime.utcnow().isoformat(),
                    'methods': list(mcs._required_methods),
                    'attributes': list(mcs._required_attributes),
                    'properties': list(mcs._required_properties)
                }
                
                # Store in Redis
                redis_client.hset(
                    mcs._registry_key,
                    class_id,
                    json.dumps(metadata)
                )
                
                # Set expiry for the registry (7 days)
                redis_client.expire(mcs._registry_key, 604800)
            
            logger.info(f"Class '{cls.__name__}' registered with ID: {class_id}")
            
        except Exception as e:
            logger.warning(f"Failed to register class in Redis: {str(e)}")
    
    @classmethod
    def _generate_class_id(mcs, cls: Type) -> str:
        """
        Generate unique ID for class.
        Genera ID único para la clase.
        """
        content = f"{cls.__module__}.{cls.__name__}:{getattr(cls, 'version', '1.0.0')}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    @classmethod
    def get_registry(mcs) -> Dict[str, Type]:
        """
        Get the class registry.
        Obtiene el registro de clases.
        """
        return mcs._registry.copy()
    
    @classmethod
    def get_class_by_id(mcs, class_id: str) -> Optional[Type]:
        """
        Get a class from registry by ID.
        Obtiene una clase del registro por ID.
        """
        return mcs._registry.get(class_id)
    
    @classmethod
    def get_redis_registry(mcs) -> Dict[str, Dict[str, Any]]:
        """
        Get registry from Redis.
        Obtiene el registro de Redis.
        """
        try:
            redis_client = mcs.get_redis_client()
            if redis_client:
                registry = redis_client.hgetall(mcs._registry_key)
                return {
                    class_id: json.loads(metadata)
                    for class_id, metadata in registry.items()
                }
            else:
                return {}
        except Exception as e:
            logger.warning(f"Could not get registry from Redis: {str(e)}")
            return {}


class APIContractBase(metaclass=APIContractMeta):
    """
    Base class for API contract enforcement.
    Clase base para cumplimiento de contrato API.
    
    All classes inheriting from this will be subject to contract validation.
    Todas las clases que hereden de esta estarán sujetas a validación de contrato.
    """
    
    __abstract__ = True  # Mark as abstract to skip registration
    
    # Default implementations of required attributes
    version = "1.0.0"
    description = "API Contract Base Class"
    author = "System"
    
    def __init__(self):
        """Initialize base class / Inicializa clase base"""
        self._metrics = {}
        self._status = "initialized"
    
    @property
    def status(self) -> str:
        """Get status / Obtiene el estado"""
        return self._status
    
    @status.setter
    def status(self, value: str):
        """Set status / Establece el estado"""
        self._status = value
    
    @property
    def metrics(self) -> Dict[str, Any]:
        """Get metrics / Obtiene métricas"""
        return self._metrics.copy()
    
    def process(self, data: Any) -> Any:
        """
        Process data - must be overridden.
        Procesa datos - debe ser sobrescrito.
        """
        raise NotImplementedError("Subclasses must implement 'process' method")
    
    def validate(self, data: Any) -> bool:
        """
        Validate data - must be overridden.
        Valida datos - debe ser sobrescrito.
        """
        raise NotImplementedError("Subclasses must implement 'validate' method")
    
    def serialize(self) -> Dict[str, Any]:
        """
        Serialize object - can be overridden.
        Serializa objeto - puede ser sobrescrito.
        """
        return {
            'class': self.__class__.__name__,
            'version': self.version,
            'status': self.status,
            'metrics': self.metrics
        }
    
    @classmethod
    def deserialize(cls, data: Dict[str, Any]):
        """
        Deserialize object - can be overridden.
        Deserializa objeto - puede ser sobrescrito.
        """
        instance = cls()
        instance._status = data.get('status', 'initialized')
        instance._metrics = data.get('metrics', {})
        return instance


def contract_class(required_methods: Optional[Set[str]] = None,
                  required_attributes: Optional[Set[str]] = None,
                  required_properties: Optional[Set[str]] = None):
    """
    Decorator to apply custom contract to a class.
    Decorador para aplicar contrato personalizado a una clase.
    """
    def decorator(cls):
        # Create custom metaclass with specific requirements
        class CustomContractMeta(APIContractMeta):
            pass
        
        # Set custom contract
        if required_methods:
            CustomContractMeta._required_methods = required_methods
        if required_attributes:
            CustomContractMeta._required_attributes = required_attributes
        if required_properties:
            CustomContractMeta._required_properties = required_properties
        
        # Create new class with custom metaclass
        return CustomContractMeta(
            cls.__name__,
            cls.__bases__,
            dict(cls.__dict__)
        )
    
    return decorator