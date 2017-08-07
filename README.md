Mockafka
=======

Mockafka is a DSL which allows testing of kafka topologies without need of a
kafka and zookeeper installation. It is a Java 8 port of [jpzk/mockedstreams](https://github.com/jpzk/mockedstreams).

Add it to your `pom.xml`...

```xml
<dependency>
  <groupId>com.github.carlosmenezes</groupId>
  <artifactId>mockafka</artifactId>
  <version>0.1</version>
  <scope>test</scope>
</dependency>
```

or to your build.gradle.

```groovy
testCompile "com.github.carlosmenezes:mockafka:0.1"
```

Examples
-------

```java
Map<Integer, Integer> input = Stream.of(1, 2, 3, 4, 5, 6, 7)
    .collect(Collectors.toMap(k -> k, v -> v));

Serde<Integer> integerSerde = Serdes.Integer();

List<ProducerRecord<Integer, Integer>> output = Mockafka
    .builder()
    .topology(builder ->
        builder.stream(integerSerde, integerSerde, "numbersTopic")
            .filter((key, value) -> value % 2 == 1)
            .to(integerSerde, integerSerde, "oddNumbersTopic")
    )
    .input("numbersTopic", integerSerde, integerSerde, input)
    .output("oddNumbersTopic", integerSerde, integerSerde, 4);

System.out.println(output);
assertEquals(4, output.size());
``` 
