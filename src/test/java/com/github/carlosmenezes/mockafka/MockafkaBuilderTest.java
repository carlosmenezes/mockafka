package com.github.carlosmenezes.mockafka;

import com.github.carlosmenezes.mockafka.exceptions.EmptyInputException;
import com.github.carlosmenezes.mockafka.exceptions.EmptyOutputSizeException;
import com.github.carlosmenezes.mockafka.exceptions.NoTopologyException;
import com.github.carlosmenezes.mockafka.util.TopologyUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MockafkaBuilderTest {

    @Test
    public void doStreamChangingValueToUpperCase() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {

        Map<String, String> output = Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, createInputKeyValue())
            .output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);

        assertTrue(output.containsKey("somekey"));
        assertEquals("SOMEVALUE", output.get("somekey"));
        assertEquals(1, output.size());
    }

    @Test
    public void doOutpuToTable() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Map<String, String> output = Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, createInputKeyValue())
            .outputTable(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);

        assertEquals("SOMEVALUE", output.get("somekey"));
    }

    @Test
    public void doSendOutputToTopicAndStore() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        MockafkaBuilder builder = Mockafka
            .builder()
            .topology(TopologyUtil::joinTopology)
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.integerSerde, createInputKeyValueB())
            .input(TopologyUtil.INPUT_TOPIC_B, TopologyUtil.stringSerde, TopologyUtil.integerSerde, createInputKeyValueB())
            .stores(TopologyUtil.STORAGE_NAME);

        Map<String, Integer> outputA = builder.output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.integerSerde, 1);
        Map<String, Integer> outputB = builder.output(TopologyUtil.OUTPUT_TOPIC_B, TopologyUtil.stringSerde, TopologyUtil.integerSerde, 1);

        assertEquals(1, outputA.size());
        assertEquals(1, outputB.size());
        assertEquals(84, (long) outputA.get("somekey"));
        assertEquals(0, (long) outputB.get("somekey"));
        assertEquals(1, builder.stateTable(TopologyUtil.STORAGE_NAME).size());
        assertEquals(42, builder.stateTable(TopologyUtil.STORAGE_NAME).get("somekey"));
    }

    @Test
    public void doNotChangeOutputOrder() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Map<Integer, Integer> input = of(1, 2, 3, 4, 5, 6, 7)
            .collect(toMap(k -> k, v -> v ));

        Serde<Integer> integerSerde = Serdes.Integer();

        Map<Integer, Integer> output = Mockafka
            .builder()
            .topology(builder ->
                builder.stream(integerSerde, integerSerde, "numbersTopic")
                    .filter((key, value) -> value % 2 == 1)
                    .to(integerSerde, integerSerde, "oddNumbersTopic")
            )
            .input("numbersTopic", integerSerde, integerSerde, input)
            .output("oddNumbersTopic", integerSerde, integerSerde, 4);

        assertEquals(4, output.size());
        assertEquals(1, (int) output.get(1));
        assertEquals(3, (int) output.get(3));
        assertEquals(5, (int) output.get(5));
        assertEquals(7, (int) output.get(7));
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

    @Test(expected = NoTopologyException.class)
    public void doThorowExceptionWhenTopologyNotInformed() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Mockafka
            .builder()
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, new HashMap<>())
            .output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);

    }

    private Map<String, String> createInputKeyValue() {
        Map<String, String> map = new HashMap<>();
        map.put("somekey", "someValue");
        return map;
    }

    private Map<String, Integer> createInputKeyValueB() {
        Map<String, Integer> map = new HashMap<>();
        map.put("somekey", 42);
        return map;
    }
}
