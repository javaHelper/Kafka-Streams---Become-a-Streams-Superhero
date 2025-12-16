package com.example;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.nio.charset.StandardCharsets;

public class KafkaSerdeUtils {

    public static <T> Serde<T> getSerde(Class<T> type, Gson gson) {
        return Serdes.serdeFrom(
                (topic, data) -> {
                    if (data == null) {
                        return null;
                    }
                    return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
                },
                (topic, data) -> {
                    if (data == null || data.length == 0) {
                        return null;
                    }
                    return gson.fromJson(new String(data, StandardCharsets.UTF_8), type);
                }
        );
    }

    public static <T> Serde<T> getSerde(Class<T> type) {
        Gson gson = new Gson();
        return Serdes.serdeFrom(
                (topic, data) -> {
                    if (data == null) {
                        return null;
                    }
                    return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
                },
                (topic, data) -> {
                    if (data == null || data.length == 0) {
                        return null;
                    }
                    return gson.fromJson(new String(data, StandardCharsets.UTF_8), type);
                }
        );
    }
}
