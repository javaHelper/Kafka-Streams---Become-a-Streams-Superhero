package com.example;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class KTableTopology {
    Gson gson = new Gson();

    @Bean
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Materialized materialized = Materialized.as("movies-quotes");
        Materialized materializedWithDefault = Materialized.as("movies-quotes-with-default");
        KTable simpleConsumerWithDefault = builder.table("star-wars-quotes");
        KTable<String, MovieQuote> materializedConsumerWithDefault = builder.table("disney-quotes", materializedWithDefault);
        KTable simpleConsumer = builder.table("arnold-swerthenagger-quotes", Consumed.with(Serdes.String(), KafkaSerdeUtils.getSerde(MovieQuote.class,gson)));
        KTable materializedConsumer = builder.table("basketball-quotes", Consumed.with(Serdes.String(), Serdes.String()), materialized);

        simpleConsumerWithDefault.toStream().to("simpleConsumerWithDefault");
        materializedConsumerWithDefault.toStream().to("materializedConsumerWithDefault");
        simpleConsumer.toStream().to("simpleConsumer");
        materializedConsumer.toStream().to("materializedConsumer");
        return builder.build();
    }
}
