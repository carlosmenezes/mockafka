package com.github.carlosmenezes.mockafka;

import com.github.carlosmenezes.mockafka.exceptions.EmptyInputException;
import com.github.carlosmenezes.mockafka.exceptions.EmptyOutputSizeException;
import com.github.carlosmenezes.mockafka.exceptions.NoTopologyException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.ProcessorTopologyTestDriver;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class MockafkaBuilder {

    private final Properties properties;
    private final List<String> stateStores;
    private final Map<String, List<Message<byte[], byte[]>>> inputs;
    private Consumer<KStreamBuilder> topology = null;

    public MockafkaBuilder(Properties properties, List<String> stateStores, Map<String, List<Message<byte[], byte[]>>> inputs) {
        this.properties = properties;
        this.stateStores = stateStores;
        this.inputs = inputs;
    }

    public MockafkaBuilder config(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public MockafkaBuilder topology(Consumer<KStreamBuilder> consumer) {
        this.topology = consumer;
        return this;
    }

    public MockafkaBuilder stores(String... stores) {
        this.stateStores.addAll(asList(stores));
        return this;
    }

    public <K, V> MockafkaBuilder input(String topic, Serde<K> keySerde, Serde<V> valueSerde, Message<K, V>... data) {
        Serializer<K> keySerializer = keySerde.serializer();
        Serializer<V> valueSerializer = valueSerde.serializer();

        List<Message<byte[], byte[]>> convertedData = Stream.of(data)
            .map(m -> new Message<>(keySerializer.serialize(topic, m.getKey()), valueSerializer.serialize(topic, m.getValue())))
            .collect(toList());

        inputs.put(topic, convertedData);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <K, V> List<Message<K, V>> output(String topic, Serde<K> keySerde, Serde<V> valueSerde, int size) throws EmptyOutputSizeException, EmptyInputException, NoTopologyException {
        if (size < 1) throw new EmptyOutputSizeException();

        return withProcessedDriver(driver ->
            range(0, size)
                .mapToObj(i -> driver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer()))
                .map(pr -> new Message<>(pr.key(), pr.value()))
                .collect(toList())
        );
    }

    public <K, V> Map<K, V> outputTable(String topic, Serde<K> keySerde, Serde<V> valueSerde, int size) throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        return output(topic, keySerde, valueSerde, size).stream()
            .collect(toMap(
                Message::getKey,
                Message::getValue,
                (v1, v2) -> { throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));},
                LinkedHashMap::new
            ));
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> stateTable(String name) throws EmptyInputException, NoTopologyException {
        return withProcessedDriver(driver -> {
            KeyValueIterator<Object, Object> records = driver.getKeyValueStore(name).all();

            Map<K, V> result = new LinkedHashMap<>();
            records.forEachRemaining(record -> result.put((K) record.key, (V) record.value));
            records.close();
            return result;
        });
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> windowStateTable(String name, K key, long timeFrom, long timeTo) throws EmptyInputException, NoTopologyException {

        return withProcessedDriver(driver -> {
            ReadOnlyWindowStore<K, V> store = (ReadOnlyWindowStore<K, V>) driver.getStateStore(name);
            WindowStoreIterator<V> records = store.fetch(key, timeFrom, timeTo);

            Map<K, V> result = new LinkedHashMap<>();
            records.forEachRemaining(record -> result.put((K) record.key, record.value));
            records.close();

            return result;
        });
    }

    private ProcessorTopologyTestDriver stream() throws NoTopologyException {
        if (topology == null) throw new NoTopologyException();

        properties.putIfAbsent(APPLICATION_ID_CONFIG, String.format("mocked-%s", UUID.randomUUID()));
        properties.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KStreamBuilder builder = new KStreamBuilder();
        topology.accept(builder);

        return new ProcessorTopologyTestDriver(new StreamsConfig(properties), builder);
    }

    private void produce(ProcessorTopologyTestDriver driver) {
        inputs.forEach((topic, input) ->
            input.forEach(m -> driver.process(topic, m.getKey(), m.getValue()))
        );
    }

    private <T> T withProcessedDriver(Function<ProcessorTopologyTestDriver, T> f) throws EmptyInputException, NoTopologyException {
        if (inputs.isEmpty()) throw new EmptyInputException();

        ProcessorTopologyTestDriver driver = stream();
        produce(driver);
        T result = f.apply(driver);
        driver.close();
        return result;
    }
}
