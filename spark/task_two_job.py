        
        
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
        .config("spark.executor.memory", "15g") \
        .config("spark.driver.memory", "15g") \
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
        #### TASK - 2 Queries ####
        ## Query - 1: /count?by=kingdom
        aggregated_kingdom_data = json_data.groupBy("dwc:kingdom").agg(count("*").alias("count"))
        ## Query - 2, 3: /count?by=species and /count?by=source
        aggregated_rest_data = json_data.groupBy().agg(
            lit(topic.upper()).alias("source"),
            approx_count_distinct("dwc:scientificName").alias("distinct_count"),
            count("*").alias("total_records")
            )
    else:
        #### TASK - 2 Queries ####
        ## Query - 1: /count?by=kingdom
        aggregated_kingdom_data = json_data.groupBy("kingdom").agg(count("*").alias("count"))
        ## Query - 2, 3: /count?by=species and /count?by=source
        aggregated_rest_data = json_data.groupBy().agg(
            lit(topic.upper()).alias("source"),
            approx_count_distinct("scientificName").alias("distinct_count"),
            count("*").alias("total_records")
        )

    topic_name = f'{topic}_query'
    king_query_name = 'kingdom_query'
    rest_query_name = 'rest_query'

    kingdom_query = aggregated_kingdom_data \
        .writeStream \
        .queryName(king_query_name) \
        .format("memory") \
        .outputMode("complete") \
        .trigger(processingTime="15 seconds") \
        .start()

    rest_query = aggregated_rest_data \
        .writeStream \
        .queryName(rest_query_name) \
        .format("memory") \
        .outputMode("complete") \
        .trigger(processingTime="15 seconds") \
        .start()

    kingdom_query.awaitTermination(60)
    rest_query.awaitTermination(60)

    data = spark.sql(f'SELECT * FROM {king_query_name}')
    data_2 = spark.sql(f'SELECT * FROM {rest_query_name}')

    pandas_df = data.toPandas()
    data_dict = pandas_df.to_dict(orient="records")
    for item in data_dict:
        if "dwc:kingdom" in item:
            item["kingdom"] = item.pop("dwc:kingdom")
    pandas_df_2 = data_2.toPandas()
    data_dict_2 = pandas_df_2.to_dict(orient="records")
    print('\n\n\n\n')
    print(data_dict)
    print('\n\n\n\n')
    print(data_dict_2)
    print('\n\n\n\n')
    
    data = {
        'kingdom': data_dict,
        'species': data_dict_2[0]['distinct_count'],
        'source_count': data_dict_2[0]['total_records'],
        'source_name': data_dict_2[0]['source']
    }
    
    # Publish data_dict to Kafka topic
    create_topic_if_not_exists(topic_name, bootstrap_servers=",".join(kafka_servers))
    publish_to_kafka([data], topic_name, bootstrap_servers=",".join(kafka_servers))
    
    spark.stop()
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some data.')
    parser.add_argument('topic', type=str, help='name of the kafka topic')
    
    args = parser.parse_args()
    
    print('Topic name: ', args.topic)
    run_spark_job(args.topic)