Mockafka
=======

Mockafka is a DSL which allows testing of kafka-streams topologies without need of a
kafka and zookeeper installation. It is a Java 8 port of [jpzk/mockedstreams](https://github.com/jpzk/mockedstreams).

Add it to your `pom.xml`...

```xml
<dependency>
  <groupId>com.github.carlosmenezes</groupId>
  <artifactId>mockafka</artifactId>
  <version>0.1.1</version>
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
Map<Integer, Integer> input = Stream.of(1, 2, 3, 4, 5, 6, 7)
    .collect(Collectors.toMap(k -> k, v -> v));

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
``` 

Multiple inputs/outputs
-------

```java
MockafkaBuilder builder = Mockafka
  .builder()
  .topology(builder -> {...})
  .input("someInput", Serdes.String(), Serdes.String(), someInput)
  .input("anotherInput", Serdes.String(), Serdes.String(), anotherInput);
  
Map<String, String> someOutput = builder.output("someOutput", Serdes.String(), Serdes.String(), 10);
Map<String, String> anotherOutput = builder.output("anotherOutput", Serdes.String(), Serdes.String(), 10);
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

