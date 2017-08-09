package com.github.carlosmenezes.mockafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TestTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {

        if (record.value() instanceof Integer) return Long.valueOf(String.valueOf(record.value()));
        return record.timestamp();
    }
}
