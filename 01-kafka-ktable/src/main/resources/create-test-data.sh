#!/bin/bash

# Kafka broker configuration
BROKER="localhost:9092"

# Function to create topic if it doesn't exist
create_topic() {
    local topic=$1
    local partitions=${2:-1}
    local replication=${3:-1}

    echo "Creating topic: $topic"
    kafka-topics --bootstrap-server $BROKER --create --topic $topic --partitions $partitions --replication-factor $replication --if-not-exists
}

# Function to produce JSON data
produce_data() {
    local topic=$1
    local key=$2
    local value=$3

    echo "$key:$value" | kafka-console-producer --bootstrap-server $BROKER --topic $topic --property "parse.key=true" --property "key.separator=:"
    echo "Produced to $topic: key=$key, value=$value"
}

# Create topics
echo "Creating Kafka topics..."
create_topic "star-wars-quotes"
create_topic "disney-quotes"
create_topic "arnold-swerthenagger-quotes"
create_topic "basketball-quotes"
create_topic "simpleConsumerWithDefault"
create_topic "materializedConsumerWithDefault"
create_topic "simpleConsumer"
create_topic "materializedConsumer"

echo ""
echo "Waiting for topics to be ready..."
sleep 3

# Star Wars Quotes (simpleConsumerWithDefault - expects String values)
echo ""
echo "Producing Star Wars quotes..."
produce_data "star-wars-quotes" "1" "\"May the Force be with you.\""
produce_data "star-wars-quotes" "2" "\"I am your father.\""
produce_data "star-wars-quotes" "3" "\"Do. Or do not. There is no try.\""
produce_data "star-wars-quotes" "4" "\"I find your lack of faith disturbing.\""
produce_data "star-wars-quotes" "5" "\"These aren't the droids you're looking for.\""

# Disney Quotes (materializedConsumerWithDefault - expects JSON MovieQuote objects)
echo ""
echo "Producing Disney quotes..."
produce_data "disney-quotes" "1" "{\"quote\":\"Hakuna Matata! It means no worries.\"}"
produce_data "disney-quotes" "2" "{\"quote\":\"To infinity and beyond!\"}"
produce_data "disney-quotes" "3" "{\"quote\":\"The past can hurt. But the way I see it, you can either run from it, or learn from it.\"}"
produce_data "disney-quotes" "4" "{\"quote\":\"All it takes is faith and trust.\"}"
produce_data "disney-quotes" "5" "{\"quote\":\"Ohana means family. Family means nobody gets left behind or forgotten.\"}"

# Arnold Schwarzenegger Quotes (simpleConsumer - expects JSON MovieQuote objects with custom serde)
echo ""
echo "Producing Arnold Schwarzenegger quotes..."
produce_data "arnold-swerthenagger-quotes" "1" "{\"quote\":\"I'll be back.\"}"
produce_data "arnold-swerthenagger-quotes" "2" "{\"quote\":\"Hasta la vista, baby.\"}"
produce_data "arnold-swerthenagger-quotes" "3" "{\"quote\":\"Get to the chopper!\"}"
produce_data "arnold-swerthenagger-quotes" "4" "{\"quote\":\"It's not a tumor!\"}"
produce_data "arnold-swerthenagger-quotes" "5" "{\"quote\":\"If it bleeds, we can kill it.\"}"

# Basketball Quotes (materializedConsumer - expects String values)
echo ""
echo "Producing basketball quotes..."
produce_data "basketball-quotes" "1" "\"I've missed more than 9000 shots in my career. I've lost almost 300 games. 26 times, I've been trusted to take the game winning shot and missed. I've failed over and over and over again in my life. And that is why I succeed.\""
produce_data "basketball-quotes" "2" "\"The strength of the team is each individual member. The strength of each member is the team.\""
produce_data "basketball-quotes" "3" "\"I can accept failure, everyone fails at something. But I can't accept not trying.\""
produce_data "basketball-quotes" "4" "\"Talent wins games, but teamwork and intelligence win championships.\""
produce_data "basketball-quotes" "5" "\"You miss 100% of the shots you don't take.\""

echo ""
echo "All test data has been produced!"
echo "Topics created and populated:"
echo "1. star-wars-quotes (String values)"
echo "2. disney-quotes (JSON MovieQuote objects)"
echo "3. arnold-swerthenagger-quotes (JSON MovieQuote objects with custom serde)"
echo "4. basketball-quotes (String values)"