package fireball.mockafka;

import fireball.mockafka.exceptions.EmptyInputException;
import fireball.mockafka.exceptions.EmptyOutputSizeException;
import fireball.mockafka.exceptions.NoTopologyException;
import fireball.mockafka.util.TopologyUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static fireball.mockafka.util.TopologyUtil.INPUT_TOPIC_A;
import static fireball.mockafka.util.TopologyUtil.INPUT_TOPIC_B;
import static fireball.mockafka.util.TopologyUtil.OUTPUT_TOPIC_A;
import static fireball.mockafka.util.TopologyUtil.OUTPUT_TOPIC_B;
import static fireball.mockafka.util.TopologyUtil.STORAGE_NAME;
import static fireball.mockafka.util.TopologyUtil.integerSerde;
import static fireball.mockafka.util.TopologyUtil.stringSerde;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MockafkaBuilderTest {

    @Test
    public void doStreamChangingValueToUpperCase() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {

        List<ProducerRecord<String, String>> output = Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .input(INPUT_TOPIC_A, stringSerde, stringSerde, createInputKeyValue())
            .output(OUTPUT_TOPIC_A, stringSerde, stringSerde, 1);

        assertEquals("somekey", output.get(0).key());
        assertEquals("SOMEVALUE", output.get(0).value());
        assertEquals(1, output.size());
    }

    @Test
    public void doOutpuToTable() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Map<String, String> output = Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .input(INPUT_TOPIC_A, stringSerde, stringSerde, createInputKeyValue())
            .outputTable(OUTPUT_TOPIC_A, stringSerde, stringSerde, 1);

        assertEquals("SOMEVALUE", output.get("somekey"));
    }

    @Test
    public void doSendOutputToTopicAndStore() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        MockafkaBuilder builder = Mockafka
            .builder()
            .topology(TopologyUtil::joinTopology)
            .input(INPUT_TOPIC_A, stringSerde, integerSerde, createInputKeyValueB())
            .input(INPUT_TOPIC_B, stringSerde, integerSerde, createInputKeyValueB())
            .stores(asList(STORAGE_NAME));

        List<ProducerRecord<String, String>> outputA = builder.output(OUTPUT_TOPIC_A, stringSerde, integerSerde, 1);
        List<ProducerRecord<String, String>> outputB = builder.output(OUTPUT_TOPIC_B, stringSerde, integerSerde, 1);

        assertEquals(1, outputA.size());
        assertEquals(1, outputB.size());
        assertEquals(84, outputA.get(0).value());
        assertEquals(0, outputB.get(0).value());
        assertEquals(1, builder.stateTable(STORAGE_NAME).size());
        assertEquals(42, builder.stateTable(STORAGE_NAME).get("somekey"));
    }

    @Test(expected = EmptyInputException.class)
    public void doThrowExceptionWhenInputNotInformed() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {

        Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .output(OUTPUT_TOPIC_A, stringSerde, stringSerde, 1);
    }

    @Test(expected = EmptyOutputSizeException.class)
    public void doThorowExceptionWhenOutputNotInformed() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Mockafka
            .builder()
            .topology(TopologyUtil::upperCaseTopology)
            .output(OUTPUT_TOPIC_A, stringSerde, stringSerde, 0);
    }

    @Test(expected = NoTopologyException.class)
    public void doThorowExceptionWhenTopologyNotInformed() throws EmptyOutputSizeException, NoTopologyException, EmptyInputException {
        Mockafka
            .builder()
            .input(INPUT_TOPIC_A, stringSerde, stringSerde, new HashMap<>())
            .output(OUTPUT_TOPIC_A, stringSerde, stringSerde, 1);

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
