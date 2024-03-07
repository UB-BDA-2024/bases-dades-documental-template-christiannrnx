from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = data

    # Get and group dynamic data
    dynamic_data = {
        "temperature": data.temperature,
        "humidity": data.humidity,
        "battery_level": data.battery_level,
        "last_seen": data.last_seen,
    }

    # Create key
    key = f"sensor:{sensor_id}:data"
    # Set dynamic data (with JSON format) to key in the redis db
    redis.set(key, json.dumps(dynamic_data))

    return db_sensordata

def get_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    
    # Get sensor by id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    # Create key
    key = f"sensor:{sensor_id}:data"
    # Get dynamic data assigned to key
    dynamic_data = json.loads(redis.get(key))

    # Group static and dynamic data
    db_sensordata = {
        "id": sensor_id,
        "name": db_sensor.name,
        "temperature": dynamic_data['temperature'],
        "humidity": dynamic_data['humidity'],
        "battery_level": dynamic_data['battery_level'],
        "last_seen": dynamic_data['last_seen']
    }

    return db_sensordata

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor