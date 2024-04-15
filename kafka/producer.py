# For configuring the producer, connecting the producer and 
# for running the producer
import sys
sys.path.append('/users/ngoyal')

from uuid import uuid4
import os
import json
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
# from src.utils.interfaces.singleton import Singleton


class MyProducer():
    
    __producer = None
    string_serializer = StringSerializer('utf_8')
    
    def __init__(self, server_address = 'localhost:9092'):
        self.__producer = Producer({
            'bootstrap.servers': server_address
        })
    
    def delivery_callback(err, msg):
        if err is not None:
            print("Delivery failed for User record {}: {}".format(msg.key(), err))
            return
        print('User record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
    def produce_topic(self, topic_name, data):
        # Serialize data dictionary to JSON string
        serialized_data = json.dumps(data)
        serialized_data_bytes = serialized_data.encode('utf-8')
        
        self.__producer.produce(topic_name, key=self.string_serializer(str(uuid4())), value=serialized_data_bytes, callback=self.delivery_callback)

        def stop_producer():
            self.__producer.stop()
        
        return stop_producer