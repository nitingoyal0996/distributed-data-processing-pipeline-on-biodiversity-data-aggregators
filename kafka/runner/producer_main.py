import sys
sys.path.append('/users/ngoyal')
import argparse

from src.kafka.producer import MyProducer
from src.utils.stream_data import StreamDataStrategy

def stream_data_from_source(server, topic_name):
    kafka_producer = MyProducer(server)
    
    stream = StreamDataStrategy(topic_name).select_stream()
    
    try:
        for item in stream.start_stream():
            # Send the JSON data with topic name = topic_name to Kafka queue
            # print(item)
            kafka_producer.produce_topic(topic_name, item)
            
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    except ValueError:
        print("Invalid input, discarding record...")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some data.')
    parser.add_argument('brokers', type=str, help='broker server addresses to use')
    parser.add_argument('topic', type=str, help='name of the kafka topic')
    
    args = parser.parse_args()
    
    print('Topic name: ', args.topic)
    print('Broker Address: ', args.brokers)
    stream_data_from_source(args.brokers, args.topic)