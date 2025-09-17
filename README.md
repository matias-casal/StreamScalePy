# StreamScale - Distributed Data Processing System

A production-ready, scalable data processing system featuring memory-efficient pipelines, distributed task scheduling, and advanced Python patterns.

## 🎯 Key Features

### 1. **Memory-Efficient Data Pipeline**
- Stream processing with constant memory usage (generators/iterators)
- FastAPI webhook for real-time JSON data ingestion
- Automatic data aggregation with sliding windows
- PostgreSQL persistence and RabbitMQ message queuing

### 2. **Custom Context Manager**
- Robust resource management for databases, queues, and APIs
- Automatic cleanup and comprehensive error handling
- Performance metrics collection
- Support for nested contexts

### 3. **Advanced Meta-Programming**
- API contract enforcement via metaclasses
- Automatic class registration in Redis
- Runtime validation with descriptive error messages
- Full inheritance support

### 4. **Lazy Evaluation Iterator**
- Memory-efficient transformations (map, filter, reduce)
- Pagination and chunking support
- Database integration for large datasets
- Composable operations with method chaining

### 5. **Distributed Task Scheduler**
- Multi-process task execution
- Priority queues and dependency management
- Dynamic worker scaling
- Task cancellation and timeout handling

## 📋 Prerequisites

### System Requirements
- **OS**: Linux, macOS, or Windows with WSL2
- **RAM**: Minimum 4GB (8GB recommended)
- **Disk**: 2GB free space
- **CPU**: 2+ cores recommended

### Software Requirements
- **Docker**: 20.10+ ([Install Docker](https://docs.docker.com/get-docker/))
- **Docker Compose**: 2.0+ (included with Docker Desktop)
- **Python**: 3.11+ (for local development)
- **Make**: GNU Make (optional, for convenience commands)

### Port Requirements
Ensure these ports are available:
- `5432`: PostgreSQL
- `6379`: Redis
- `5672`: RabbitMQ
- `15672`: RabbitMQ Management UI
- `8000`: FastAPI Application
- `80`: Nginx (optional)
- `8080`: PgAdmin (optional)
- `9090`: Prometheus (optional)
- `3000`: Grafana (optional)

## 🚀 Quick Start (5 Minutes)

### 1. Clone & Setup
```bash
# Clone repository
git clone <repository-url>
cd StreamScalePy

# Create environment file
cp .env.example .env
```

### 2. Start All Services
```bash
# Using Make (recommended)
make up

# OR using Docker Compose directly
docker-compose up -d
```

### 3. Verify Installation
```bash
# Check all services are running
make status

# Test API health
curl http://localhost:8000/health

# Expected response:
# {"status":"healthy","components":{"processor":true,"storage":true,"queue":true}}
```

## 🧪 Testing Each Module

### Module 1: Memory-Efficient Data Pipeline

```bash
# Test 1: Send single event
curl -X POST http://localhost:8000/webhook/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "test_001",
    "source": "sensor_1",
    "event_type": "temperature",
    "data": {"temperature": 25.5, "humidity": 60}
  }'

# Test 2: Send batch of events (memory-efficient processing)
python3 -c "
import httpx
import json

# Generate 1000 events
events = [
    {
        'event_id': f'evt_{i:04d}',
        'source': f'sensor_{i % 10}',
        'event_type': 'measurement',
        'data': {'value': i * 1.5, 'count': i}
    }
    for i in range(1000)
]

# Send batch
response = httpx.post(
    'http://localhost:8000/webhook/batch',
    json=events,
    timeout=30
)
print(f'Status: {response.status_code}')
print(f'Response: {response.json()}')
"

# Test 3: Check aggregations
curl http://localhost:8000/stats | python3 -m json.tool

# Monitor memory usage (should stay constant)
docker stats streamscale_api --no-stream
```

### Module 2: Custom Context Manager

```python
# test_context_manager.py
from src.context_manager.resource_manager import ResourceManager

# Test nested contexts and automatic cleanup
with ResourceManager(enable_metrics=True) as rm:
    # Acquire multiple resources
    db = rm.acquire_database()
    redis = rm.acquire_redis()
    rabbitmq = rm.acquire_rabbitmq()
    
    # Nested context manager
    with rm.nest(ResourceManager()) as nested_rm:
        nested_db = nested_rm.acquire_database(name="analytics")
        print("Nested resources acquired")
    
    # Resources automatically cleaned up
    print("Metrics:", rm.get_metrics())
    
# Test error handling
try:
    with ResourceManager() as rm:
        db = rm.acquire_database(host="invalid_host")
except ConnectionError as e:
    print(f"Handled error: {e}")
```

Run test:
```bash
docker exec -it streamscale_api python3 test_context_manager.py
```

### Module 3: Advanced Meta-Programming

```python
# test_metaclass.py
from src.meta_programming.api_contract import APIContractBase, APIContractMeta

# Test 1: Valid implementation
class DataProcessor(APIContractBase):
    version = "1.0.0"
    description = "Processes sensor data"
    author = "Test User"
    
    def process(self, data):
        return data.upper() if isinstance(data, str) else str(data)
    
    def validate(self, data):
        return data is not None

# Test 2: Contract violation (will raise error)
try:
    class InvalidProcessor(APIContractBase):
        version = "1.0.0"
        # Missing required attributes and methods!
        pass
except ContractViolationError as e:
    print(f"Contract violation detected: {e}")

# Test 3: Check auto-registration
registry = APIContractMeta.get_registry()
print(f"Registered classes: {list(registry.keys())}")

# Test 4: Redis persistence
redis_registry = APIContractMeta.get_redis_registry()
print(f"Classes in Redis: {redis_registry}")
```

Run test:
```bash
docker exec -it streamscale_api python3 test_metaclass.py
```

### Module 4: Lazy Iterator

```python
# test_lazy_iterator.py
from src.iterator.lazy_collection import LazyCollection, DatabaseLazyCollection

# Test 1: Basic lazy evaluation
print("Test 1: Lazy evaluation with large dataset")
large_range = LazyCollection(range(1_000_000))
result = (large_range
    .filter(lambda x: x % 2 == 0)  # Even numbers
    .map(lambda x: x ** 2)         # Square them
    .filter(lambda x: x < 1000)    # Less than 1000
    .take(5)                        # Take first 5
    .collect())                     # Materialize
print(f"Result: {result}")

# Test 2: Memory efficiency with infinite sequence
def fibonacci():
    a, b = 0, 1
    while True:
        yield a
        a, b = b, a + b

fib = LazyCollection(fibonacci())
fib_result = (fib
    .take(100)
    .filter(lambda x: x % 2 == 0)
    .take(10)
    .collect())
print(f"Even Fibonacci: {fib_result}")

# Test 3: Batch processing
data = LazyCollection(range(1, 101))
batches = data.batch(10).take(3).collect()
print(f"Batches: {batches}")

# Test 4: Database lazy loading
db_collection = DatabaseLazyCollection.from_table(
    "processed_events",
    columns=["event_id", "data"],
    limit=1000,
    fetch_size=100  # Fetch 100 rows at a time
)

# Process without loading all into memory
processed = (db_collection
    .map(lambda row: row['data'])
    .filter(lambda data: data is not None)
    .take(10)
    .collect())
print(f"DB results: {processed}")
```

Run test:
```bash
docker exec -it streamscale_api python3 test_lazy_iterator.py
```

### Module 5: Distributed Task Scheduler

```python
# test_scheduler.py
import time
from src.scheduler.scheduler import DistributedScheduler
from src.scheduler.task import TaskPriority

# Initialize scheduler
scheduler = DistributedScheduler(num_workers=4)
scheduler.initialize()

# Test 1: Simple task submission
def add_numbers(x, y):
    time.sleep(1)  # Simulate work
    return x + y

task_id = scheduler.submit_task(
    add_numbers,
    args=(5, 3),
    name="addition",
    priority=TaskPriority.HIGH,
    timeout=30
)
print(f"Submitted task: {task_id}")

# Test 2: Task with dependencies
def fetch_data():
    return [1, 2, 3, 4, 5]

def process_data(data):
    return sum(data)

parent_id = scheduler.submit_task(fetch_data, name="fetch")
child_id = scheduler.submit_task(
    process_data,
    dependencies={parent_id},
    name="process"
)

# Test 3: Dynamic scaling
print("Current workers: 4")
scheduler.scale_workers(8)
print("Scaled to 8 workers")

# Test 4: Monitor execution
scheduler.run()  # Start processing

# Check status
time.sleep(5)
status = scheduler.get_task_status(task_id)
print(f"Task status: {status}")

# Get statistics
stats = scheduler.get_statistics()
print(f"Scheduler stats: {stats}")

# Test 5: Task cancellation
cancel_id = scheduler.submit_task(
    lambda: time.sleep(100),
    name="long_task"
)
scheduler.cancel_task(cancel_id)
print(f"Task {cancel_id} cancelled")

scheduler.shutdown()
```

Run test:
```bash
docker exec -it streamscale_api python3 test_scheduler.py
```

## 📊 Performance Testing

### Run Performance Tests
```bash
# Test with batch processing
python3 -c "import httpx; events=[{'event_id':f'test_{i}','source':f'sensor_{i%10}','event_type':'test','data':{'value':i}} for i in range(1000)]; print(httpx.post('http://localhost:8000/webhook/batch',json=events,timeout=30).json())"
```

### Expected Performance Metrics
- **Pipeline Processing**: ~8,000-10,000 events/second
- **Memory Usage**: ~100MB for API container
- **Task Scheduling**: Distributed across 4 workers
- **Lazy Iterator**: Processes large datasets with constant memory

## 🔧 Configuration

### Environment Variables (.env)
```env
# Database
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=streamscale
POSTGRES_USER=streamscale_user
POSTGRES_PASSWORD=streamscale_password

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# RabbitMQ
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

# API
API_PORT=8000
API_WORKERS=4

# Scheduler
SCHEDULER_WORKERS=4
SCHEDULER_MAX_TASKS=1000
SCHEDULER_TASK_TIMEOUT=300
```

## 🛠️ Development

### Local Development Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests in container
docker exec streamscale_api pytest src/tests/ -v
```

### Useful Commands
```bash
# View logs
make logs           # All services
make logs-api       # API only
make logs-scheduler # Scheduler only

# Access containers
make shell-api      # API shell
make shell-postgres # PostgreSQL shell
make shell-redis    # Redis CLI

# Scale workers
make scale-workers n=8

# Database operations  
docker exec streamscale_postgres pg_dump -U streamscale_user streamscale > backup.sql
```

## 🐛 Troubleshooting

### Issue: Services won't start
```bash
# Check port conflicts
sudo lsof -i :5432  # PostgreSQL
sudo lsof -i :6379  # Redis
sudo lsof -i :8000  # API

# Reset everything
make clean
make build
make up
```

### Issue: Database connection errors
```bash
# Check PostgreSQL is running
docker exec streamscale_postgres pg_isready

# Check credentials
docker exec streamscale_api env | grep POSTGRES

# Reset database
docker exec streamscale_postgres psql -U streamscale_user -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

### Issue: RabbitMQ not accepting messages
```bash
# Access RabbitMQ Management UI
open http://localhost:15672  # User: guest, Pass: guest

# Reset queues
docker exec streamscale_rabbitmq rabbitmqctl reset
```

### Issue: High memory usage
```bash
# Check container stats
docker stats

# Limit container memory
docker-compose down
# Edit docker-compose.yml, add to services:
# deploy:
#   resources:
#     limits:
#       memory: 512M
docker-compose up -d
```

### Issue: Tests failing
```bash
# Run tests in verbose mode
pytest src/tests/ -vvs

# Run specific test
pytest src/tests/test_integration.py::TestPipeline -v

# Check test database
docker exec streamscale_postgres psql -U streamscale_user -c "\dt"
```

## 📈 Monitoring

### Access Available Tools
- **API Documentation**: http://localhost:8000/docs
- **RabbitMQ Management**: http://localhost:15672 (guest/guest)

### Key Metrics to Monitor
1. **Pipeline Performance**
   - Events processed per second
   - Average processing time
   - Queue depth

2. **Resource Usage**
   - Memory consumption per container
   - CPU usage
   - Database connections

3. **Task Scheduler**
   - Tasks completed/failed
   - Worker utilization
   - Queue wait time

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────┐
│                    Load Balancer (Nginx)               │
└────────────────────────┬───────────────────────────────┘
                         │
┌────────────────────────▼───────────────────────────────┐
│                   FastAPI Application                   │
│  • Webhook endpoints for data ingestion                 │
│  • RESTful API for system interaction                   │
│  • Async request handling                               │
└────────────┬───────────────────────┬───────────────────┘
             │                       │
┌────────────▼─────────┐  ┌─────────▼──────────────────┐
│   Data Pipeline      │  │   Task Scheduler           │
│ • Generator-based    │  │ • Priority queue mgmt      │
│ • Stream processing  │  │ • Dependency resolution    │
│ • Sliding windows    │  │ • Worker pool management   │
└──────┬───────────────┘  └──────────┬─────────────────┘
       │                              │
┌──────▼────────────────────────────────▼────────────────┐
│              Distributed Infrastructure                 │
├──────────────┬──────────────┬──────────────────────────┤
│ PostgreSQL   │   Redis      │   RabbitMQ               │
│ • Events     │ • Cache      │ • Task queue             │
│ • Aggregations│ • Registry  │ • Result queue           │
│ • Metrics    │ • Priority Q │ • Event streaming        │
└──────────────┴──────────────┴──────────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   Worker Pool     │
                    │ • Auto-scaling    │
                    │ • Multi-process   │
                    │ • Fault tolerant  │
                    └───────────────────┘
```

## 🚀 Production Considerations

**Note:** This system is a technical demonstration. For production deployment, you would need to:
- Implement authentication and authorization
- Set up proper secrets management
- Configure SSL/TLS certificates
- Add database migration system
- Implement monitoring and alerting
- Configure automated backups

## 📚 API Documentation

### Core Endpoints

#### Health Check
```http
GET /health
```

#### Ingest Single Event
```http
POST /webhook/ingest
Content-Type: application/json

{
  "event_id": "evt_001",
  "source": "sensor_1",
  "event_type": "temperature",
  "data": {
    "value": 25.5
  }
}
```

#### Ingest Batch
```http
POST /webhook/batch
Content-Type: application/json

[
  {"event_id": "evt_001", "source": "sensor_1", ...},
  {"event_id": "evt_002", "source": "sensor_2", ...}
]
```

#### Get Statistics
```http
GET /stats
```

#### Clean Old Data
```http
DELETE /data/cleanup?days_old=30
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for your changes
4. Ensure all tests pass (`make test`)
5. Format your code (`make format`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👥 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Contact the development team

## 🙏 Acknowledgments

- Built for the Argentine developer community 🇦🇷
- Demonstrates advanced Python patterns and distributed systems design
- Special thanks to all contributors

---

**Note**: This is a technical challenge implementation showcasing production-ready patterns for scalable data processing systems.