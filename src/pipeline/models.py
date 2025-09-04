"""
Data models for the pipeline.
Modelos de datos para el pipeline.
"""
from datetime import datetime
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class DataEvent(BaseModel):
    """
    Incoming data event model.
    Modelo de evento de datos entrante.
    """
    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str = Field(..., description="Data source identifier")
    event_type: str = Field(..., description="Type of event")
    data: Dict[str, Any] = Field(default_factory=dict, description="Event payload")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Event metadata")
    
    @field_validator('data')
    @classmethod
    def validate_data_not_empty(cls, v):
        """Ensure data is not empty / Asegura que los datos no estén vacíos"""
        if not v:
            raise ValueError("Event data cannot be empty")
        return v
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "event_id": "evt_123456",
                "source": "sensor_001",
                "event_type": "temperature_reading",
                "data": {
                    "temperature": 25.5,
                    "humidity": 60.0,
                    "location": "room_1"
                }
            }
        }
    )


class AggregatedData(BaseModel):
    """
    Aggregated data model.
    Modelo de datos agregados.
    """
    aggregation_id: str = Field(..., description="Aggregation identifier")
    window_start: datetime = Field(..., description="Aggregation window start")
    window_end: datetime = Field(..., description="Aggregation window end")
    source: str = Field(..., description="Data source")
    event_type: str = Field(..., description="Event type")
    count: int = Field(default=0, description="Number of events")
    metrics: Dict[str, float] = Field(default_factory=dict, description="Aggregated metrics")
    metadata: Optional[Dict[str, Any]] = Field(default=None)


class ProcessedEventDB(Base):
    """
    SQLAlchemy model for processed events.
    Modelo SQLAlchemy para eventos procesados.
    """
    __tablename__ = "processed_events"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    source = Column(String, index=True)
    event_type = Column(String, index=True)
    data = Column(JSON)
    processed_at = Column(DateTime, default=datetime.utcnow)
    aggregation_id = Column(String, nullable=True, index=True)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary / Convertir a diccionario"""
        return {
            "id": self.id,
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "source": self.source,
            "event_type": self.event_type,
            "data": self.data,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "aggregation_id": self.aggregation_id
        }


class AggregationDB(Base):
    """
    SQLAlchemy model for aggregations.
    Modelo SQLAlchemy para agregaciones.
    """
    __tablename__ = "aggregations"
    
    id = Column(Integer, primary_key=True, index=True)
    aggregation_id = Column(String, unique=True, index=True)
    window_start = Column(DateTime, index=True)
    window_end = Column(DateTime, index=True)
    source = Column(String, index=True)
    event_type = Column(String, index=True)
    event_count = Column(Integer, default=0)
    metrics = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary / Convertir a diccionario"""
        return {
            "id": self.id,
            "aggregation_id": self.aggregation_id,
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None,
            "source": self.source,
            "event_type": self.event_type,
            "event_count": self.event_count,
            "metrics": self.metrics,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }