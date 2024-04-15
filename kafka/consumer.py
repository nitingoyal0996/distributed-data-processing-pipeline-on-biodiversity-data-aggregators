import sys
sys.path.append('/users/ngoyal')

import os
from confluent_kafka import Consumer
# from src.utils.interfaces.singleton import Singleton

class MyConsumer():
    
    
    def __init__(self):
        
        conf = {
            'bootstrap.servers': '1@128.110.217.192:9092,2@128.110.217.163:9093,3@128.110.217.175:9094',
            'group.id': 'local_consumer'
        }
        self.my_consumer = Consumer(conf)
                
    def consume_topic(self, topic_name):
        print(topic_name)
        self.my_consumer.subscribe([topic_name])
