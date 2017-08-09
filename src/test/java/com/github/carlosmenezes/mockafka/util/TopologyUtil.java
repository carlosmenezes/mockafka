package com.github.carlosmenezes.mockafka.util;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;

import static com.github.carlosmenezes.mockafka.MockafkaBuilderTest.TEST_STORE;

public class TopologyUtil {

    public static Serde<String> stringSerde = Serdes.String();
    public static Serde<Integer> integerSerde = Serdes.Integer();

    public static final String INPUT_TOPIC_A = "inputTopicA";
    public static final String OUTPUT_TOPIC_A = "outputTopicA";

    public static final String INPUT_TOPIC_B = "inputTopicB";
    public static final String OUTPUT_TOPIC_B = "outputTopicB";

    public static final String WINDOW_TOPIC = "windowTopic";

    public static final String STORAGE_NAME = "SomeStorage";

    public static KStreamBuilder upperCaseTopology(KStreamBuilder builder) {

        builder
            .stream(stringSerde, stringSerde, INPUT_TOPIC_A)
            .mapValues(String::toUpperCase)
            .to(stringSerde, stringSerde, OUTPUT_TOPIC_A);

        return builder;
    }

    public static KStreamBuilder joinTopology(KStreamBuilder builder) {
        KStream<String, Integer> kStreamA = builder.stream(stringSerde, integerSerde, INPUT_TOPIC_A);
        KStream<String, Integer> kStreamB = builder.stream(stringSerde, integerSerde, INPUT_TOPIC_B);

        KTable<String, Integer> table = kStreamA
            .groupByKey(stringSerde, integerSerde)
            .aggregate(() -> 0, (k, v, t) -> v, integerSerde, STORAGE_NAME);

        kStreamB
            .leftJoin(table, (v1, v2) -> v1 + v2, stringSerde, integerSerde)
            .to(stringSerde, integerSerde, OUTPUT_TOPIC_A);

        kStreamB
            .leftJoin(table, (v1, v2) -> v1 - v2, stringSerde, integerSerde)
            .to(stringSerde, integerSerde, OUTPUT_TOPIC_B);

        return builder;
    }

    public static KStreamBuilder windowStateTopology(KStreamBuilder builder) {
        builder.stream(stringSerde, integerSerde, WINDOW_TOPIC)
            .groupByKey(stringSerde, integerSerde)
            .count(TimeWindows.of(1), TEST_STORE);

        return builder;
    }
}
