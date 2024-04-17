#!/bin/bash

kafka-topics.sh --bootstrap-server 128.110.217.192:9092,128.110.217.163:9092,128.110.217.175:9092 --create --topic 'gbif' --partitions 1 --config retention.ms=10000