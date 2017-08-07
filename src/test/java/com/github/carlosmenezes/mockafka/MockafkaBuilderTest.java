package com.github.carlosmenezes.mockafka;

import com.github.carlosmenezes.mockafka.exceptions.EmptyInputException;
import com.github.carlosmenezes.mockafka.exceptions.EmptyOutputSizeException;
import com.github.carlosmenezes.mockafka.exceptions.NoTopologyException;
import com.github.carlosmenezes.mockafka.util.TopologyUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MockafkaBuilderTest {

    @Test
    public void doStreamChangingValueToUpperCase() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {

        List<ProducerRecord<String, String>> output = Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .input(TopologyUtil.INPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, createInputKeyValue())
            .output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.stringSerde, 1);

        assertEquals("somekey", output.get(0).key());
        assertEquals("SOMEVALUE", output.get(0).value());
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
            .stores(asList(TopologyUtil.STORAGE_NAME));

        List<ProducerRecord<String, String>> outputA = builder.output(TopologyUtil.OUTPUT_TOPIC_A, TopologyUtil.stringSerde, TopologyUtil.integerSerde, 1);
        List<ProducerRecord<String, String>> outputB = builder.output(TopologyUtil.OUTPUT_TOPIC_B, TopologyUtil.stringSerde, TopologyUtil.integerSerde, 1);

        assertEquals(1, outputA.size());
        assertEquals(1, outputB.size());
        assertEquals(84, outputA.get(0).value());
        assertEquals(0, outputB.get(0).value());
        assertEquals(1, builder.stateTable(TopologyUtil.STORAGE_NAME).size());
        assertEquals(42, builder.stateTable(TopologyUtil.STORAGE_NAME).get("somekey"));
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
