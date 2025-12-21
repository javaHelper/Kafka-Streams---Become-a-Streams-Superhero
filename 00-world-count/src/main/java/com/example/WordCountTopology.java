package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class WordCountTopology {
    @Value("${input-topic}")
    private String inputTopic;

    @Value("${output-topic}")
    private String outputTopic;

    @Bean
    public Topology kafkaStreamsTopology(StreamsBuilder builder){
        KStream<String, String> textLines = builder.stream(inputTopic);

        KTable<String, Long> wordsCountKTable = textLines
                .flatMapValues(value -> List.of(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count();

        wordsCountKTable
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
