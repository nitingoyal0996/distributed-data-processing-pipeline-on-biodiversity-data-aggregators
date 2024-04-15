        
        
import sys
sys.path.append('/users/ngoyal')

import os
import json
import datetime

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.schemas import gbif_schema, obis_schema, idigbio_schema
from src.spark.publish_to_kafka import publish_to_kafka, create_topic_if_not_exists

def run_spark_job(topic = 'gbif'):
    
    schema_map = {
        'gbif': gbif_schema.schema,
        'idigbio': idigbio_schema.schema,
        'obis': obis_schema.schema
    }

    kafka_servers = ['128.110.217.192:9092','128.110.217.163:9092','128.110.217.175:9092']

    spark = SparkSession.builder \
        .appName("Streaming From Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.cores", "4")\
        .config("spark.executor.instances", "1")\
        .config("spark.executor.memory", "5g") \
        .config("spark.driver.memory", "5g") \
        .getOrCreate()

    # Read data from Kafka in a streaming DataFrame
    data = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(kafka_servers)) \
        .option("subscribe", topic) \
        .load()

    base_data = data.selectExpr("CAST(value AS STRING)")

    # Parse JSON data using corresponding schema
    json_data = base_data.withColumn("value", from_json(base_data["value"], schema_map[topic])).select("value.*")

    aggregated_data = json_data \
                .groupBy() \
                .agg(
                    lit(topic.upper()).alias("Source"),
                    count(when(col("kingdom") == "Animalia", 1)).alias("Number of Animal Records"),
                    count(when(col("kingdom") == "Plantae", 1)).alias("Number of Plant Records"),
                    count(when(col("kingdom") == "Fungi", 1)).alias("Number of Fungi Records"),
                    approx_count_distinct("scientificName").alias("Total Number of Unique Species")
                )

    query_name = f'{topic}_query'

    # Start the query
    query = aggregated_data \
        .writeStream \
        .queryName(query_name) \
        .format("memory") \
        .outputMode("complete") \
        .start()

    query.awaitTermination(50)

    data = spark.sql(f'SELECT * FROM {query_name}')

    pandas_df = data.toPandas()
    data_dict = pandas_df.to_dict(orient="records")
    print(data_dict)
    
    # Publish data_dict to Kafka topic
    topic_name = query_name
    create_topic_if_not_exists(topic_name)
    publish_to_kafka(data_dict, topic_name)

    # if not os.path.exists('spark_api_outputs'):
    #     os.makedirs('spark_api_outputs')
    # timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # file_path = f'{topic}_output_{timestamp}.json'
    # with open(file_path, "w") as f:
    #     json.dump(data_dict, f)
    #     print(f"File saved: {file_path}")

    spark.stop()
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some data.')
    parser.add_argument('topic', type=str, help='name of the kafka topic')
    
    args = parser.parse_args()
    
    print('Topic name: ', args.topic)
    run_spark_job(args.topic)