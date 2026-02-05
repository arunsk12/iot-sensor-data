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
    sensor_id = random.randint(1, 100)
    temperature = round(random.uniform(20.0, 30.0), 2)
    humidity = round(random.uniform(40.0, 60.0), 2)
    timestamp = int(time.time() * 1000) # Milliseconds
    return {
        "sensor_id": f"sensor_{sensor_id:03d}",
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": timestamp
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
        producer.produce(KAFKA_TOPIC, key=data["sensor_id"].encode('utf-8'), value=json_data, callback=delivery_report)
        producer.poll(0) # Serve delivery callback queue.
        time.sleep(1) # Send data every 1 second
except KeyboardInterrupt:
    pass
finally:
    print("Flushing producer messages...")
    producer.flush()
    print("Simulator stopped.")