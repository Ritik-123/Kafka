from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json

# Define kafka producer
producer= KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer= lambda v: json.dumps(v).encode('utf-8')
)

# Produce a message
for i in range(100000):
    future= producer.send('test-topic', {'key': 'One topic to send a message value'})

try:
    record_metadata= future.get(timeout= 10)
except KafkaError as e:
    print(f"Error in sending message: {e}")
finally:
    producer.close()