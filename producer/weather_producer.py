import requests
import json
import time
from kafka import KafkaProducer

API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 52.52, 13.41  # Berlin

KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:29092"  # réseau Docker (INTERNAL listener)

def fetch_weather():
    params = {
        "latitude": LAT,
        "longitude": LON,
        "current_weather": "true",
    }
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("current_weather", {})

def transform_weather(record: dict) -> dict:
    # Convertir Celsius en Fahrenheit
    if "temperature" in record:
        record["temp_f"] = record["temperature"] * 9/5 + 32

    # Alerte vent fort
    record["high_wind_alert"] = record.get("windspeed", 0) > 10
    return record

def build_producer() -> KafkaProducer:
    """
    kafka-python peut échouer sur l'auto-détection de version (check_version).
    On force api_version et on retry jusqu'à ce que le broker soit joignable.
    """
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                api_version=(2, 6, 0),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=20000,
                api_version_auto_timeout_ms=20000,
                retries=10,
            )
            return producer
        except Exception as e:
            print("Kafka not ready yet, retrying in 5s...", repr(e))
            time.sleep(5)

def main():
    producer = build_producer()
    print("Weather streaming producer started...")

    while True:
        try:
            weather = fetch_weather()
            if weather:
                transformed = transform_weather(weather)
                producer.send(KAFKA_TOPIC, transformed)
                producer.flush()
                print("Sent to weather_transformed:", transformed)
        except Exception as e:
            print("Error fetching or sending weather:", e)

        time.sleep(30)  # fetch every 30 seconds

if __name__ == "__main__":
    main()
