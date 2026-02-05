import json
import time
from confluent_kafka import Producer
import random

# Kafka configuration
KAFKA_BROKER = 'kafka-1:9092'  # This is the internal Docker network address
KAFKA_TOPIC = 'iot_sensor_data'

# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'python-simulator'
}

producer = Producer(producer_conf)

print(f"Starting data simulator, sending to Kafka broker: {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")

def generate_sensor_data():
    sensor_id_num = random.randint(1, 100)
    device_id = f"device_{sensor_id_num:03d}"
    timestamp = int(time.time() * 1000)

    # Determine if this message should be an anomaly
    is_anomaly = random.random() < 0.1 # 10% chance of an anomaly

    if is_anomaly:
        sensor_type_choice = random.choice(["temperature", "humidity"])
        if sensor_type_choice == "temperature":
            # Generate an anomalous temperature (e.g., very high or very low)
            value = random.choice([round(random.uniform(101.0, 150.0), 2), round(random.uniform(-20.0, -1.0), 2)])
            sensor_type = "temperature"
        else: # humidity
            # Generate an anomalous humidity (e.g., very high or very low)
            value = random.choice([round(random.uniform(101.0, 120.0), 2), round(random.uniform(-10.0, -1.0), 2)])
            sensor_type = "humidity"
    else:
        # Generate normal data
        sensor_type_choice = random.choice(["temperature", "humidity"])
        if sensor_type_choice == "temperature":
            value = round(random.uniform(20.0, 30.0), 2)
            sensor_type = "temperature"
        else: # humidity
            value = round(random.uniform(60.0, 80.0), 2)
            sensor_type = "humidity"

    location = f"room_{random.randint(1, 5)}"
    battery_level = round(random.uniform(50.0, 100.0), 2)

    return {
        "device_id": device_id,
        "sensor_type": sensor_type,
        "value": value,
        "timestamp": timestamp,
        "location": location,
        "battery_level": battery_level
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

try:
    while True:
        data = generate_sensor_data()
        json_data = json.dumps(data).encode('utf-8')
        print(f"Generated JSON message: {json_data}")
        producer.produce(KAFKA_TOPIC, key=data["device_id"].encode('utf-8'), value=json_data, callback=delivery_report)
        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    print("Flushing producer messages...")
    producer.flush()
    print("Simulator stopped.")