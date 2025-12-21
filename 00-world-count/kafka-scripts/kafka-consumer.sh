kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic words-output \
    --from-beginning \
    --property print.key=true \
    --property print.value=true \
    --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
    --value-deserializer org.apache.kafka.common.serialization.LongDeserializer