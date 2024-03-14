import json
import math
from fastapi import HTTPException
from sqlalchemy.orm import Session
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from typing import List, Optional

from . import models, schemas

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

    collection = mongodb.client["mydatabase"]["sensors"]
    
    mongodb_sensor = {
        "id": db_sensor.id,
        "name": sensor.name,
        "latitude": sensor.latitude,
        "longitude": sensor.longitude,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version
    }
    
    x = collection.insert_one(mongodb_sensor)
    
    print(x.inserted_id)

    return db_sensor

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = data

    # Get and group dynamic data
    dynamic_data = {
        "velocity": data.velocity,
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

def get_data(redis: RedisClient, sensor_id: int, db: Session) -> schemas.Sensor:
    
    # Get sensor by id
    db_sensor = get_sensor(db, sensor_id)
    # Create key
    key = f"sensor:{sensor_id}:data"
    # Get dynamic data assigned to key
    dynamic_data = json.loads(redis.get(key))

    # Group static and dynamic data
    db_sensordata = {
        "id": sensor_id,
        "name": db_sensor.name,
        "velocity": dynamic_data['velocity'],
        "temperature": dynamic_data['temperature'],
        "humidity": dynamic_data['humidity'],
        "battery_level": dynamic_data['battery_level'],
        "last_seen": dynamic_data['last_seen']
    }

    return db_sensordata

def delete_sensor(db: Session, sensor_id: int):
    
    db_sensor = get_sensor(db, sensor_id)

    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(db: Session, mongodb: MongoDBClient, redis: RedisClient, latitude: float, longitude: float, radius: float):

    # Convertimos la distancia del radio a una medida en grados de latitud y longitud
    # (Esta conversión es aproximada y puede no ser precisa en todos los casos)
    degrees_per_km = 1 / 111.12
    lat_deg = radius * degrees_per_km
    lon_deg = radius * degrees_per_km / math.cos(latitude * math.pi / 180)

    # Calculamos los límites de latitud y longitud para la búsqueda de sensores cercanos
    lat_min = latitude - lat_deg
    lat_max = latitude + lat_deg
    lon_min = longitude - lon_deg
    lon_max = longitude + lon_deg

     # Conexión a la base de datos MongoDB
    collection = mongodb.client["mydatabase"]["sensors"]


    # Consulta para encontrar sensores dentro de los límites especificados
    query = {
        "latitude": {"$gte": lat_min, "$lte": lat_max},
        "longitude": {"$gte": lon_min, "$lte": lon_max}
    }

    # Realizar la consulta a MongoDB
    sensors = collection.find(query)

    # Convertir el resultado en una lista de diccionarios para facilitar el formato de retorno
    sensors_nearby = []
    for sensor in sensors:
        print(sensor)
        print(sensor['id'])
        sensors_nearby.append(get_data(redis,sensor['id'],db))
    
    print(sensors_nearby)
    print(len(sensors_nearby))
        

    return sensors_nearby
