Mockafka
=======

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/2719b4636f134b359fa13ba33fed24ad)](https://www.codacy.com/app/pachecomenezes/mockafka?utm_source=github.com&utm_medium=referral&utm_content=carlosmenezes/mockafka&utm_campaign=badger)

Mockafka is a DSL which allows testing of kafka-streams topologies without need of a
kafka and zookeeper installation. It is a Java 8 port of [jpzk/mockedstreams](https://github.com/jpzk/mockedstreams).

Add it to your `pom.xml`...

```xml
<dependency>
  <groupId>com.github.carlosmenezes</groupId>
  <artifactId>mockafka</artifactId>
  <version>0.1.2</version>
  <scope>test</scope>
</dependency>
```

or to your `build.gradle`.

```groovy
testCompile "com.github.carlosmenezes:mockafka:0.1.1"
```

Examples
-------

```java
List<Message<Integer, Integer>> input = Stream.of(1, 2, 3, 4, 5, 6, 7)
    .map(i -> new Message<>(i, i))
    .collect(Collectors.toList());

Serde<Integer> integerSerde = Serdes.Integer();

List<Message<Integer, Integer>> output = Mockafka
    .builder()
    .topology(builder ->
        builder.stream(integerSerde, integerSerde, "numbersTopic")
            .filter((key, value) -> value % 2 == 1)
            .to(integerSerde, integerSerde, "oddNumbersTopic")
    )
    .input("numbersTopic", integerSerde, integerSerde, input)
    .output("oddNumbersTopic", integerSerde, integerSerde, 4);

List<Message<Integer, Integer>> expected = Arrays.asList(new Message<>(1, 1), new Message<>(3, 3), new Message<>(5, 5), new Message<>(7, 7));
assertEquals(4, output.size());
assertEquals(expected, output);
``` 

Multiple inputs/outputs
-------

```java
MockafkaBuilder builder = Mockafka
  .builder()
  .topology(builder -> {...})
  .input("someInput", Serdes.String(), Serdes.String(), someInput)
  .input("anotherInput", Serdes.String(), Serdes.String(), anotherInput);
  
List<Message<String, String>> someOutput = builder.output("someOutput", Serdes.String(), Serdes.String(), 10);
List<Message<String, String>> anotherOutput = builder.output("anotherOutput", Serdes.String(), Serdes.String(), 10);
```

Using State Stores
-------

You can create state stores using `.stores(String... stores)` method and verify
it's contents with `.stateTable(String name)` method:

```java
MockafkaBuilder builder = Mockafka
  .builder()
  .topology(builder -> {...})
  .input("someInput", Serdes.String(), Serdes.String(), someInput)
  .stores("someStore");
  
Map<String, String> someStore = builder.stateTable("someStore");  
assertEquals(10, someStore.size());
```

Using Window State Stores
-------

When defining a timestamp extractor in `.config(Properties config)` you can verify the content as 
windowed state stores using the method `.windowStateTable(String name, K key, long timeFrom, long timeTo)`.

```java
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
```

Configuring Streams
-------

If you need some custom configuration, just use the method `.config(Properties config)`:

```java
Properties properties = new Properties();
properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TestTimestampExtractor.class.getName());

MockafkaBuilder builder = Mockafka
    .builder()
    .topology({...})
    .input({...})
    .stores({...})
    .config(properties);
```
