package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.regex.Pattern;

@Component
public class KStreamTopology {

    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> singleTopic = builder.stream("star-wars-quotes",
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> multipleTopic = builder.stream(Arrays.asList("disney-quotes","arnold-swerthenagger-quotes"),
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> patternTopic = builder.stream(Pattern.compile("basketball.*"),
                Consumed.with(Serdes.String(), Serdes.String()));

        singleTopic.to("singleTopic");
        multipleTopic.to("multipleTopic");
        patternTopic.to("patternTopic");

        return builder.build();
    }
}
