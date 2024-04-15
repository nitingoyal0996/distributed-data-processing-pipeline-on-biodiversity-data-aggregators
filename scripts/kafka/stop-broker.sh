#!/bin/bash

# Path to Kafka bin directory
kafka_bin_dir="/opt/kafka/bin"

# Kafka configuration file for KRaft
kraft_server_properties="/opt/kafka/config/kraft/server.properties"

# Stop the broker
"$kafka_bin_dir/kafka-server-stop.sh" "$kraft_server_properties"