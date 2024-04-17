import sys
sys.path.append('/users/ngoyal')

from confluent_kafka import Consumer
# from src.utils.interfaces.singleton import Singleton

class MyConsumer():
    
    def __init__(self, servers):
        
        conf = {
            'bootstrap.servers': servers,
            'group.id': 'local_consumer'
        }
        self.my_consumer = Consumer(conf)
        print(conf)
        
        
    def print_assignment(self, consumer, partitions):
        print('Assignment:', consumer)

      
    def consume_topic(self, topic_name):
        print(topic_name)
        self.my_consumer.subscribe([topic_name], on_assign=self.print_assignment)
