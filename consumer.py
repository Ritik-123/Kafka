from kafka import KafkaConsumer
import json

# Define Kafka Consumer
consumer= KafkaConsumer(
    'test-topic', # The topic to consume message from
    bootstrap_servers= ['localhost:9092'], # List of kafka brokers to connect to
    auto_offset_reset= 'earliest', # Where to start reading messages when no offset is stored ('earliest' to read from the beginning)
    enable_auto_commit= True, #Automatically commit offsets after consuming messages
    value_deserializer= lambda x: x.decode('utf-8') if x else None, # Deserialize the message values from bytes to utf-8 strings
)   

# Consume messages with error handling for non-json messages
for message in consumer:
    print(message.value)