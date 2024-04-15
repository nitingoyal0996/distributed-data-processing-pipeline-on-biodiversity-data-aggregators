#!/bin/bash

# Path to Kafka bin directory
kafka="/opt/kafka/bin"
# Kafka configuration file for KRaft
kraft_server_properties="/opt/kafka/config/kraft/server.properties"

# Check if meta.properties file exists in /tmp/kraft* folder
meta_properties_file=$(find /tmp/kraft* -name 'meta.properties')

if [ -n "$meta_properties_file" ]; then
    # Parse cluster.id from meta.properties file
    cluster_id=$(grep -Po 'cluster.id=\K[^ ]+' "$meta_properties_file")
    echo "Found cluster ID from meta.properties: $cluster_id"
else
    echo "meta.properties file not found. Continuing with creation of new cluster_id."
    # Generate an ID for the cluster
    cluster_id="$("$kafka/kafka-storage.sh" random-uuid)"
    echo $cluster_id
fi

# update the cluster id for the broker
"$kafka/kafka-storage.sh" format -t "$cluster_id" -c "$kraft_server_properties"

# launch the broker
"$kafka/kafka-server-start.sh" "$kraft_server_properties"
