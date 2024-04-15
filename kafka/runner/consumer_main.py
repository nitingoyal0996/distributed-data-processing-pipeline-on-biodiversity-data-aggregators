# Examples:
# https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/consumer.py
# https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/json_consumer.py

import sys
from confluent_kafka import KafkaException
sys.path.append('/users/ngoyal')

# If we have the json schema of the message
# from confluent_kafka.serialization import SerializationContext, MessageField
# from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import findspark
findspark.init()

import os
import json
from src.kafka.consumer import MyConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

def map_func(record):
    bio_record = json.loads(record.value().decode('utf-8'))
    if bio_record is not None:
        kingdom = bio_record['kingdom']
        scientific_name = bio_record['scientificName']
        return (kingdom, (1, set([scientific_name])))  # Map to (kingdom, (count, set of unique scientific names))

def reduce_func(new_values, state):
    count = state[0] if state is not None else 0
    unique_names = state[1] if state is not None else set()
    
    for val in new_values:
        count += val[0]
        unique_names = unique_names.union(val[1])
    
    return (count, unique_names)

def consumer_main():
    
    # TODO: integrate spark here to do MapReduce
    consumer = MyConsumer()
    consumer.consume_topic('gbif')
    spark = SparkSession.builder.appName('BioRecordAnalysis').getOrCreate()

    try:
        while True:
            msg = consumer.my_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                """ 
                bio_record = json.loads(msg.value().decode('utf-8'))
                if bio_record is not None:
                    kingdom = bio_record['kingdom']
                    scientificName = bio_record['scientificName']
                    print(f'Bio record Kingdom: {kingdom}, Scientific Name: {scientificName}') 
                """
                rdd = spark.sparkContext.parallelize([msg])
                
                mapped_rdd = rdd.map(map_func)
                
                result = mapped_rdd.reducedByKey(reduce_func).collect()
                
                # TODO: integrate the data tool and write data to destination
                # collection = //
                # collection.write()
                # TODO: check the usecases.
                # consumer.my_consumer.store_offsets(msg)
                
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    except json.decoder.JSONDecodeError:
        print(json.decoder.JSONDecodeError.msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.my_consumer.close()

if __name__ == '__main__':
   consumer_main()
