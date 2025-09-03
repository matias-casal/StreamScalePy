# StreamScale - Distributed Data Processing System

A production-ready, scalable data processing system featuring memory-efficient pipelines, distributed task scheduling, and advanced Python patterns.

## 🚀 Features

### 1. **Memory-Efficient Data Pipeline**
- Stream processing with constant memory usage
- FastAPI webhook for real-time data ingestion
- Automatic data aggregation with sliding windows
- PostgreSQL persistence and RabbitMQ message queuing

### 2. **Custom Context Manager**
- Robust resource management for databases, queues, and APIs
- Automatic cleanup and error handling
- Performance metrics collection
- Support for nested contexts

### 3. **Advanced Meta-Programming**
- API contract enforcement via metaclasses
- Automatic class registration in Redis
- Runtime validation and metrics
- Inheritance support

### 4. **Lazy Evaluation Iterator**
- Memory-efficient transformations (map, filter, reduce)
- Pagination and chunking support
- Database integration for large datasets
- Composable operations

### 5. **Distributed Task Scheduler**
- Multi-process task execution
- Priority queues and dependency management
- Dynamic worker scaling
- Task cancellation and timeout handling

## 🛠️ Technology Stack

- **Python 3.11+** - Core language
- **FastAPI** - Web framework
- **PostgreSQL** - Primary database
- **Redis** - Caching and priority queues
- **RabbitMQ** - Message queuing
- **Docker** - Containerization
- **Nginx** - Load balancing

## 📦 Installation

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Make (optional, for convenience commands)

### Quick Start

1. **Clone the repository:**
```bash
git clone <repository-url>
cd StreamScalePy
```

2. **Copy environment configuration:**
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. **Start all services:**
```bash
make up
# Or without make:
docker-compose up -d
```

4. **Verify services are running:**
```bash
make status
# Or:
docker-compose ps
```

5. **Check API health:**
```bash
curl http://localhost:8000/health
```

## 🔧 Configuration

### Environment Variables

Key configuration options in `.env`:

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

# RabbitMQ
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672

# API
API_PORT=8000
API_WORKERS=4

# Scheduler
SCHEDULER_WORKERS=4
SCHEDULER_MAX_TASKS=1000
```

## 📝 Usage Examples

### 1. Data Pipeline - Webhook Ingestion

```python
import httpx

# Send single event
response = httpx.post(
    "http://localhost:8000/webhook/ingest",
    json={
        "event_id": "evt_001",
        "source": "sensor_1",
        "event_type": "temperature",
        "data": {
            "temperature": 25.5,
            "humidity": 60
        }
    }
)

# Send batch
response = httpx.post(
    "http://localhost:8000/webhook/batch",
    json=[
        {"event_id": f"evt_{i}", "source": "sensor_1", 
         "event_type": "measurement", "data": {"value": i}}
        for i in range(100)
    ]
)
```

### 2. Context Manager - Resource Management

```python
from src.context_manager.resource_manager import ResourceManager

with ResourceManager(enable_metrics=True) as rm:
    # Acquire database connection
    db = rm.acquire_database()
    
    # Acquire Redis connection
    redis = rm.acquire_redis()
    
    # Acquire RabbitMQ connection
    rabbitmq = rm.acquire_rabbitmq()
    
    # Resources are automatically cleaned up
    
# Get performance metrics
metrics = rm.get_metrics()
print(metrics)
```

### 3. Meta-Programming - API Contract

```python
from src.meta_programming.api_contract import APIContractBase

class DataProcessor(APIContractBase):
    version = "1.0.0"
    description = "Processes data"
    author = "Your Name"
    
    def process(self, data):
        return data.upper()
    
    def validate(self, data):
        return isinstance(data, str)

# Class is automatically registered and validated
processor = DataProcessor()
result = processor.process("hello")  # Returns "HELLO"
```

### 4. Lazy Iterator - Efficient Processing

```python
from src.iterator.lazy_collection import LazyCollection

# Process large dataset efficiently
collection = LazyCollection(range(1_000_000))
result = (collection
    .filter(lambda x: x % 2 == 0)  # Even numbers
    .map(lambda x: x ** 2)         # Square them
    .take(10)                       # Take first 10
    .collect())                     # Materialize

# Database integration
from src.iterator.lazy_collection import DatabaseLazyCollection

db_collection = DatabaseLazyCollection.from_table(
    "processed_events",
    columns=["event_id", "data"],
    where="created_at > '2024-01-01'",
    fetch_size=1000
)

# Process without loading all data
aggregated = (db_collection
    .map(lambda row: row['data']['value'])
    .filter(lambda v: v > 100)
    .batch(50)
    .collect())
```

### 5. Task Scheduler - Distributed Processing

```python
from src.scheduler.scheduler import DistributedScheduler
from src.scheduler.task import TaskPriority

scheduler = DistributedScheduler(num_workers=4)
scheduler.initialize()

# Submit simple task
def process_data(x, y):
    return x + y

task_id = scheduler.submit_task(
    process_data,
    args=(5, 3),
    priority=TaskPriority.HIGH,
    timeout=30
)

# Submit tasks with dependencies
parent_id = scheduler.submit_task(fetch_data, name="fetch")
child_id = scheduler.submit_task(
    process_data,
    dependencies={parent_id},
    name="process"
)

# Check task status
status = scheduler.get_task_status(task_id)
print(status)

# Scale workers dynamically
scheduler.scale_workers(8)  # Scale to 8 workers
```

## 🧪 Testing

### Run All Tests
```bash
make test
# Or:
python -m pytest src/tests/ -v
```

### Run Specific Test Categories
```bash
# Unit tests
make test-unit

# Integration tests
make test-integration

# Performance tests
python -m pytest src/tests/test_integration.py::TestPerformance -v
```

## 📊 Monitoring

### Access Monitoring Services

- **RabbitMQ Management**: http://localhost:15672 (guest/guest)
- **API Documentation**: http://localhost:8000/docs
- **Prometheus**: http://localhost:9090 (with `make monitor`)
- **Grafana**: http://localhost:3000 (with `make monitor`)

### View Logs
```bash
# All services
make logs

# Specific service
make logs-api
make logs-scheduler
make logs-worker
```

## 🚀 Deployment

### Production Deployment

1. **Update production configuration:**
```bash
cp .env.example .env.production
# Edit with production values
```

2. **Build optimized images:**
```bash
docker-compose -f docker-compose.yml build --no-cache
```

3. **Deploy with scaling:**
```bash
docker-compose up -d --scale worker=4
```

### Scaling Workers

```bash
# Scale worker service
make scale-workers n=8

# Or directly:
docker-compose up -d --scale worker=8
```

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   FastAPI       │────▶│   PostgreSQL    │     │     Redis       │
│   Webhook       │     └─────────────────┘     └─────────────────┘
└────────┬────────┘              │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Pipeline  │────▶│   Aggregation   │────▶│  Task Scheduler │
│   (Streaming)   │     │    Engine       │     │  (Distributed)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐                             ┌─────────────────┐
│   RabbitMQ      │◀────────────────────────────│   Worker Pool   │
│  Message Queue  │                             │   (Scalable)    │
└─────────────────┘                             └─────────────────┘
```

## 🛠️ Maintenance

### Database Backup
```bash
make backup-db
# Creates backup_YYYYMMDD_HHMMSS.sql
```

### Database Restore
```bash
make restore-db file=backup_20240101_120000.sql
```

### Clean Up
```bash
# Stop and remove all containers, volumes
make clean
```

## 📚 Documentation

- [API Documentation](http://localhost:8000/docs) - Interactive API docs
- [Architecture Guide](docs/architecture.md) - System design details
- [Performance Tuning](docs/performance.md) - Optimization tips

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👥 Authors

- StreamScale Development Team

## 🙏 Acknowledgments

- Built with love for the Argentine developer community 🇦🇷
- Special thanks to all contributors

---

**Note**: This is a technical challenge implementation demonstrating advanced Python patterns and distributed systems design.