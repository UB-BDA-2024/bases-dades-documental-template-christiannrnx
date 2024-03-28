from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient
from . import models, schemas
import json
from typing import Dict

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient) -> models.Sensor:

    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    
    # Select database
    mongodb.getDatabase("mydatabase")
    # Select collection
    mongodb.getCollection("sensors")

    # Save data in mongodb
    sensor_data = sensor.dict()
    mongodb.insertDoc(sensor_data)

    return db_sensor

def record_data(sensor_id: int, data: schemas.SensorData, db: Session, redis: RedisClient, mongodb: MongoDBClient) -> schemas.Sensor:
    
    db_sensor = get_sensor(db,sensor_id)
    
    # Get dynamic data in json format
    dynamic_data = data.json()
    # Set dynamic data in Redis
    redis.set(sensor_id, dynamic_data)
    # Format dynamic data to dict
    dynamic_data = json.loads(dynamic_data)
    
    # Select database
    mongodb.getDatabase("mydatabase")
    # Select collection
    mongodb.getCollection("sensors")
    # Find sensor in mongodb
    sensor_data = mongodb.findDoc({'name': db_sensor.name})
    # Pop mongodb id
    sensor_data.pop('_id', None)
    
    # Merge sensor data from mongodb and dynamic from redis
    db_sensordata = {**dynamic_data, **sensor_data}
    # Format sensor data into json
    db_sensordata = json.dumps(db_sensordata)

    return db_sensordata

def get_data(sensor_id: int, sensor_name: str, redis: Session, mongodb: MongoDBClient) -> schemas.Sensor:
    
    # Get dynamic data
    dynamic_data = redis.get(sensor_id)
    # Format dynamic data to dict
    dynamic_data = json.loads(dynamic_data)
    
    # Select database
    mongodb.getDatabase("mydatabase")
    # Select collection
    mongodb.getCollection("sensors")
    # Find sensor in mongodb
    sensor_data = mongodb.findDoc({'name': sensor_name})
    # Pop mongodb id
    sensor_data.pop('_id', None)
    
    # Merge sensor data from mongodb and dynamic from redis
    db_sensordata = {**dynamic_data, **sensor_data}
    # Add the sensor id
    db_sensordata['id'] = sensor_id

    return db_sensordata

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(latitude: float, longitude: float, radius: float, db: Session, redis: Session, mongodb: MongoDBClient):
    
    # Query to select only sensors that are in the range
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
     "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}
    
    # Select database
    mongodb.getDatabase("mydatabase")
    # Select collection
    mongodb.getCollection("sensors")

    # Find all sensors that match the query
    db_sensors = mongodb.collection.find(query)

    # List to save all near senssors
    sensors_near = []

    # For each sensor in the db
    for sensor in db_sensors:

        #Get sensor data
        db_sensor = get_sensor_by_name(db,sensor['name'])
        data = get_data(sensor_id=db_sensor.id, sensor_name=db_sensor.name, redis=redis, mongodb=mongodb)
        
        # Save near sensors in list
        sensors_near.append(data)    

    return sensors_near