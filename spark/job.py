from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("StreamingSpeciesCounter") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

    # Define the schema for the input data
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("value", StructType([
            StructField("scientificName", StringType(), True),
            StructField("kingdom", StringType(), True)
        ]), True),
        StructField("topic", StringType(), True)
    ])

    print(schema)

    # Read data from Kafka in a streaming DataFrame
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,128.110.217.163:9092,128.110.217.175:9092") \
        .option("subscribe", "gbif") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")) \
        .select("data.value.scientificName", "data.value.kingdom", "data.timestamp", "data.topic")

    print(df)


    # Apply watermark to the DataFrame
    withWatermark_df = df.withWatermark("timestamp", "2 minutes")

    # Define 2-minute windows
    windowed_df = withWatermark_df

    # Aggregate species_type_count
    species_type_count_df = windowed_df.groupBy(window(col("timestamp"), "2 minutes"), col("kingdom")).agg(count(col("kingdom")).alias("species_type_count"))

    # Aggregate records_per_source
    records_per_source_df = windowed_df.groupBy(window(col("timestamp"), "2 minutes"), col("topic")).agg(count(col("topic")).alias("records_per_source"))

    # Aggregate unique_species_count
    unique_species_count_df = windowed_df.groupBy(window(col("timestamp"), "2 minutes")).agg(approx_count_distinct(col("scientificName")).alias("unique_species_count"))


    flattened_species_type_count_df = species_type_count_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("kingdom"),
        col("species_type_count")
    )

    # Start the query
    query = flattened_species_type_count_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("header", "true") \
        .option("path", "outputs") \
        .option("checkpointLocation", "checkpoints") \
        .trigger(processingTime="2 Minutes") \
        .start()
    

    query.awaitTermination()
