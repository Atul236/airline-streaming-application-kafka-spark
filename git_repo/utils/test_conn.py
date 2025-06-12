from kafka import KafkaAdminClient, KafkaProducer
import logging

# Enable debug logging for better visibility
# logging.basicConfig(level=logging.DEBUG)

# Define Kafka broker (Adjust if necessary)
KAFKA_BROKER = "kafka:29092"

# Step 1: Check Kafka metadata retrieval
try:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topics = admin.list_topics()
    print(f"Kafka connection successful! Available topics: {topics}")
except Exception as e:
    print(f"Kafka metadata error: {e}")

# Step 2: Try sending a test message
try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    producer.send("test_topic", b"atul has send message to producer Test Connection Message")
    producer.flush()
    print("Test message sent successfully!")
except Exception as e:
    print(f"Kafka producer error: {e}")
