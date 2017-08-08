package com.github.carlosmenezes.mockafka;

import com.github.carlosmenezes.mockafka.exceptions.EmptyInputException;
import com.github.carlosmenezes.mockafka.exceptions.EmptyOutputSizeException;
import com.github.carlosmenezes.mockafka.exceptions.NoTopologyException;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class MockafkaBuilder {

    private final Properties properties;
    private final List<String> stateStores;
    private final Map<String, MockafkaInput> inputs;
    private Optional<Consumer<KStreamBuilder>> topology = Optional.empty();

    public MockafkaBuilder(Properties properties, List<String> stateStores, Map<String, MockafkaInput> inputs) {
        this.properties = properties;
        this.stateStores = stateStores;
        this.inputs = inputs;
    }

    public MockafkaBuilder config(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public MockafkaBuilder topology(Consumer<KStreamBuilder> consumer) {
        this.topology = Optional.of(consumer);
        return this;
    }


    public MockafkaBuilder stores(List<String> stores) {
        this.stateStores.addAll(stores);
        return this;
    }

    public <K, V> MockafkaBuilder input(String topic, Serde<K> keySerde, Serde<V> valueSerde, Map<K, V> data) {
        Serializer<K> keySerializer = keySerde.serializer();
        Serializer<V> valueSerializer = valueSerde.serializer();

        Map<byte[], byte[]> convertedData = data.entrySet().stream()
            .collect(toMap(
                e -> keySerializer.serialize(topic, e.getKey()),
                e -> valueSerializer.serialize(topic, e.getValue()),
                (v1, v2) -> { throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));},
                LinkedHashMap::new));

        inputs.put(topic, new MockafkaInput(convertedData));
        return this;
    }

    public <K, V> Map<K, V> output(String topic, Serde<K> keySerde, Serde<V> valueSerde, int size) throws EmptyOutputSizeException, EmptyInputException, NoTopologyException {
        if (size < 1) throw new EmptyOutputSizeException();

        return withProcessedDriver(driver ->
            range(0, size).mapToObj(i -> {
                Optional<ProducerRecord<K, V>> output = Optional.ofNullable(driver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer()));
                return output.isPresent() ? output.get() : Optional.empty();
            })
            .collect(toMap(
                pr -> ((ProducerRecord<K, V>) pr).key(),
                pr -> ((ProducerRecord<K, V>) pr).value(),
                (v1, v2) -> { throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));},
                LinkedHashMap::new
            ))
        );
    }

    public <K, V> Map<K, V> outputTable(String topic, Serde<K> keySerde, Serde<V> valueSerde, int size) throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        return output(topic, keySerde, valueSerde, size);
    }

    public <K, V> Map<K, V> stateTable(String name) throws EmptyInputException, NoTopologyException {
        return withProcessedDriver(driver -> {
            KeyValueIterator<Object, Object> records = driver.getKeyValueStore(name).all();

            Map<K, V> result = new LinkedHashMap<>();
            records.forEachRemaining(record -> result.put((K) record.key, (V) record.value));
            records.close();
            return result;
        });
    }

    public <K, V> Map<K, V> windowStateTable(String name, K key, long from, long timeTo) throws EmptyInputException, NoTopologyException {

        return withProcessedDriver(driver -> {
            ReadOnlyWindowStore<K, V> store = (ReadOnlyWindowStore<K, V>) driver.getStateStore(name);
            WindowStoreIterator<V> records = store.fetch(key, from, timeTo);

            Map<K, V> result = new LinkedHashMap<>();
            records.forEachRemaining(record -> result.put((K) record.key, record.value));
            records.close();

            return result;
        });
    }

    private ProcessorTopologyTestDriver stream() throws NoTopologyException {

        properties.putIfAbsent(APPLICATION_ID_CONFIG, String.format("mocked-%s", UUID.randomUUID()));
        properties.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KStreamBuilder builder = new KStreamBuilder();
        Consumer<KStreamBuilder> builderConsumer = topology.orElseThrow(NoTopologyException::new);
        builderConsumer.accept(builder);

        return new ProcessorTopologyTestDriver(new StreamsConfig(properties), builder);
    }

    private void produce(ProcessorTopologyTestDriver driver) {
        inputs.forEach((topic, input) ->
            input.input.forEach((key, value) -> driver.process(topic, key, value))
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

    public class MockafkaInput {
        private final Map<byte[], byte[]> input;

        public MockafkaInput(Map<byte[], byte[]> input) {
            this.input = input;
        }
    }
}
