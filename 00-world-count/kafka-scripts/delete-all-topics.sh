#!/bin/bash

BOOTSTRAP_SERVER="localhost:9092"

# Get all topics
TOPICS=$(kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --list)

# Delete each topic
for topic in $TOPICS; do
    echo "Deleting topic: $topic"
    kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --delete --topic "$topic"
done