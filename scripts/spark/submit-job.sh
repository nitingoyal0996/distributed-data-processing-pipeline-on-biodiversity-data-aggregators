#!/bin/bash 
spark-submit --master spark://ms1132.utah.cloudlab.us:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 src/spark/stream.py