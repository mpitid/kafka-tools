
# Kafka tools

Simple command line utilities for interacting with Kafka 0.7.x brokers.

These are similar to the scripts bundled with Kafka, but meant to interact with individual brokers instead of connecting to zookeeper.

## Building

You need to have maven 3.x to build the tools. Run `mvn clean package -P shade` to produce shaded jars under the consumer and producer target directories. These can then be run with e.g.

```bash
java -jar consumer/target/consumer.jar --help
```

Alternatively, if you have GNU make installed, run `make` to build the shaded jars and bundle them into executable scripts under the `bin` directory.

Finally, Maven can generate an RPM by running `mvn package`. Once installed, that will create its own `kafka-consumer` and `kafka-producer` scripts.

## Examples

1.  Using the shaded scripts:

    ```bash
    echo -e "hello\nworld" | ./bin/producer.sh -s localhost:9091 -t test
    ./bin/consumer.sh > p0.json -s localhost:9091 -p 0 -t test --last-offset
      30
    ```

2.  or using the scripts installed by the RPM:

    ```bash
    echo -e "hello\nworld" | kafka-producer -s localhost:9091 -t test
    kafka-consumer > p0.json -s localhost:9091 -p 0 -t test --last-offset
      30
    ```
