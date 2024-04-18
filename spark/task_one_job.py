        
        
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

def run_spark_job(topic):
    print(topic)
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
        .getOrCreate()

    # Read data from Kafka in a streaming DataFrame
    data = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(kafka_servers)) \
        .option("subscribe", topic) \
        .load()

    base_data = data.selectExpr("CAST(value AS STRING)")

    json_data = base_data.withColumn("value", from_json(base_data["value"], schema_map[topic])).select("value.*")
    
    if topic == "idigbio":
        #### TASK - 1 QUERY ####
        aggregated_data = json_data \
            .groupBy() \
            .agg(
                lit(topic.upper()).alias("Source"),
                count(when(lower(col("dwc:kingdom")) == "animalia", 1)).alias("Number of Animal Records"),
                count(when(lower(col("dwc:kingdom")) == "plantae", 1)).alias("Number of Plant Records"),
                count(when(lower(col("dwc:kingdom")) == "fungi", 1)).alias("Number of Fungi Records"),
                approx_count_distinct("dwc:scientificName").alias("Total Number of Unique Species"),
                count("*").alias("Total Records")
            )
    else:
        #### TASK - 1 QUERY ####
        aggregated_data = json_data \
            .groupBy() \
            .agg(
                lit(topic.upper()).alias("Source"),
                count(when(lower(col("kingdom")) == "animalia", 1)).alias("Number of animal records"),
                count(when(lower(col("kingdom")) == "plantae", 1)).alias("Number of plant records"),
                count(when(lower(col("kingdom")) == "fungi", 1)).alias("Number of fungi records"),
                approx_count_distinct("scientificName").alias("Total number of unique species"),
            )

    # DEBUGGER QUERY: Print each record using foreachBatch in Structured Streaming
    # def print_records_batch(df, epoch_id):
    #     for row in df.collect():
    #         print(row)
    # query = base_data.writeStream \
    #     .foreachBatch(print_records_batch) \
    #     .start()
    # query.awaitTermination(30)

    topic_name = f'{topic}_query'

    # Start the query
    query = aggregated_data \
        .writeStream \
        .queryName(topic_name) \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime="120 seconds") \
        .start()

    query.awaitTermination(1200)

    # data = spark.sql(f'SELECT * FROM {topic_name}')

    # pandas_df = data.toPandas()
    # data_dict = pandas_df.to_dict(orient="records")
    # print('\n\n\n\n')
    # print(data_dict)
    # print('\n\n\n\n')
    
    # # Publish data_dict to Kafka topic
    # create_topic_if_not_exists(topic_name, bootstrap_servers= ",".join(kafka_servers))
    # publish_to_kafka(data_dict, topic_name, bootstrap_servers= ",".join(kafka_servers))
    
    # spark.stop()
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some data.')
    parser.add_argument('topic', type=str, help='name of the kafka topic')
    
    args = parser.parse_args()
    
    print('Topic name: ', args.topic)
    run_spark_job(args.topic)