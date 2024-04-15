from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json

def create_topic_if_not_exists(topic_name, bootstrap_servers='localhost:9092'):
    # Create AdminClient with the Kafka broker address
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Check if the topic already exists
    topics = admin_client.list_topics().topics
    if topic_name not in topics:
        # Create the topic if it doesn't exist
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print("Topic created:", topic_name)
    else:
        print("Topic already exists:", topic_name)

def publish_to_kafka( data_dict, topic_name, bootstrap_servers='localhost:9092'):
    # Kafka producer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,  # Kafka broker address
        'client.id': 'python-producer'
    }

    # Create Kafka producer instance
    producer = Producer(**conf)

    try:
        # Publish each record to Kafka topic
        for record in data_dict:
            producer.produce(topic_name, json.dumps(record))
        producer.flush()
        print("Data published to Kafka topic:", topic_name)
    except Exception as e:
        print("Failed to publish data to Kafka:", str(e))

