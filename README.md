Mockafka
=======

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/2719b4636f134b359fa13ba33fed24ad)](https://www.codacy.com/app/pachecomenezes/mockafka?utm_source=github.com&utm_medium=referral&utm_content=carlosmenezes/mockafka&utm_campaign=badger)

Mockafka is a DSL which allows testing of kafka topologies without need of a
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
