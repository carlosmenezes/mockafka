package com.github.carlosmenezes.mockafka;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Properties;

public class Mockafka {

    private Mockafka() {
    }

    public static MockafkaBuilder builder() {
        return new MockafkaBuilder(new Properties(), new ArrayList<String>(), new LinkedHashMap<String, MockafkaBuilder.MockafkaInput>());
    }
}
