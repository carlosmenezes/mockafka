package com.github.carlosmenezes.mockafka;

import com.github.carlosmenezes.mockafka.exceptions.EmptyInputException;
import com.github.carlosmenezes.mockafka.exceptions.EmptyOutputSizeException;
import com.github.carlosmenezes.mockafka.exceptions.NoTopologyException;
import com.github.carlosmenezes.mockafka.util.TopologyUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.of;
import static org.junit.Assert.assertEquals;

public class MockafkaBuilderTest {

    public static final String TEST_STORE = "testStore";

    @SuppressWarnings("unchecked")
    @Test
    public void doStreamChangingValueToUpperCase() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {

        List<Message<String, String>> output = Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, createInputKeyValue())
            .output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);

        assertEquals("somekey", output.get(0).getKey());
        assertEquals("SOMEVALUE", output.get(0).getValue());
        assertEquals(1, output.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doOutpuToTable() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Map<String, String> output = Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, createInputKeyValue())
            .outputTable(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);

        assertEquals("SOMEVALUE", output.get("somekey"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doSendOutputToTopicAndStore() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        MockafkaBuilder builder = Mockafka
            .builder()
            .topology(TopologyUtil::joinTopology)
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.integerSerde, createInputKeyValueB())
            .input(TopologyUtil.INPUT_TOPIC_B, TopologyUtil.stringSerde, TopologyUtil.integerSerde, createInputKeyValueB())
            .stores(TopologyUtil.STORAGE_NAME);

        List<Message<String, Integer>> outputA = builder.output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.integerSerde, 1);
        List<Message<String, Integer>> outputB = builder.output(TopologyUtil.OUTPUT_TOPIC_B, TopologyUtil.stringSerde, TopologyUtil.integerSerde, 1);

        assertEquals(1, outputA.size());
        assertEquals(1, outputB.size());
        assertEquals(84, (long) outputA.get(0).getValue());
        assertEquals(0, (long) outputB.get(0).getValue());
        assertEquals(1, builder.stateTable(TopologyUtil.STORAGE_NAME).size());
        assertEquals(42, builder.stateTable(TopologyUtil.STORAGE_NAME).get("somekey"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doNotChangeOutputOrder() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        List<Message<Integer, Integer>> input = of(1, 2, 3, 4, 5, 6, 7)
            .map(i -> new Message<>(i, i))
            .collect(toList());

        Serde<Integer> integerSerde = Serdes.Integer();

        List<Message<Integer, Integer>> output = Mockafka
            .builder()
            .topology(builder ->
                builder.stream(integerSerde, integerSerde, "numbersTopic")
                    .filter((key, value) -> value % 2 == 1)
                    .to(integerSerde, integerSerde, "oddNumbersTopic")
            )
            .input("numbersTopic", integerSerde, integerSerde, input.toArray(new Message[]{}))
            .output("oddNumbersTopic", integerSerde, integerSerde, 4);

        assertEquals(4, output.size());
        assertEquals(1, (int) output.get(0).getValue());
        assertEquals(3, (int) output.get(1).getValue());
        assertEquals(5, (int) output.get(2).getValue());
        assertEquals(7, (int) output.get(3).getValue());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doOutputToWindowStateTable() throws EmptyInputException, NoTopologyException {

        Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TestTimestampExtractor.class.getName());

        MockafkaBuilder builder = Mockafka
            .builder()
            .topology(TopologyUtil::windowStateTopology)
            .input(TopologyUtil.WINDOW_TOPIC, TopologyUtil.stringSerde, TopologyUtil.integerSerde, createInputKeyValueForWindow().toArray(new Message[]{}))
            .stores(TEST_STORE)
            .config(properties);

        Map<String, Long> actualSomeKey = builder.windowStateTable(TEST_STORE, "somekey", 0, Long.MAX_VALUE);
        Map<Long, Long> expectedSomeKey = new HashMap<>();
        expectedSomeKey.put(25L, 1L);
        expectedSomeKey.put(42L, 2L);
        assertEquals(expectedSomeKey, actualSomeKey);

        Map<String, Long> actualAnotherKey = builder.windowStateTable(TEST_STORE, "anotherkey", 0, Long.MAX_VALUE);
        Map<Long, Long> expectedAnotherKey = new HashMap<>();
        expectedAnotherKey.put(50L, 2L);
        expectedAnotherKey.put(90L, 1L);
        assertEquals(expectedAnotherKey, actualAnotherKey);

    }

    @Test(expected = EmptyInputException.class)
    public void doThrowExceptionWhenInputNotInformed() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {

        Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);
    }

    @Test(expected = EmptyOutputSizeException.class)
    public void doThorowExceptionWhenOutputNotInformed() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 0);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NoTopologyException.class)
    public void doThorowExceptionWhenTopologyNotInformed() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Mockafka
            .builder()
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, new Message<>("", ""))
            .output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);

    }

    private Message<String, String> createInputKeyValue() {
        return new Message<>("somekey", "someValue");
    }

    private Message<String, Integer> createInputKeyValueB() {
        return new Message<>("somekey", 42);
    }

    @SuppressWarnings("unchecked")
    private List<Message<String, Integer>> createInputKeyValueForWindow() {

        return asList(
            new Message<>("somekey", 42),
            new Message<>("somekey", 25),
            new Message<>("somekey", 42),

            new Message<>("anotherkey", 50),
            new Message<>("anotherkey", 90),
            new Message<>("anotherkey", 50)
        );
    }
}
