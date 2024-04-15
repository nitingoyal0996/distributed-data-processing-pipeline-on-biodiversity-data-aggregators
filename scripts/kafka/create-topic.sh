#!/bin/bash

kafka-topics.sh --bootstrap-server 1@128.110.217.192:9092,2@128.110.217.163:9092,3@128.110.217.175:9092 --create --topic 'gbif' --partitions 1