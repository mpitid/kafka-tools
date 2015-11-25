
# Kafka tools

Simple command line utilities for interacting with Kafka 0.8.x brokers.

The functionality is similar to that of the scripts bundled with Kafka, but with a consistent and convenient interface, broken down into four broad categories:

### Pushing

Push data into a topic and partition with tunable consistency and without having to worry (too much) about leader assignment. The input format is flexible but limited to textual and line-delimited entries, in raw text or JSON.

### Pulling

Pull data out of a topic and partition without having to worry about leader assignment. The output format is flexible but limited to textual output, either raw text or JSON.

In addition, the earliest or latest logical offset for a topic can be retrieved, as well as the latest offset before a point in time.

### Topic management

List all topics, create new topics with a specific number of partitions, replicas and minimum ISR, update topics, or list detailed status information about topics and partitions such as leader, ISR and replica assignment. Deleting topics is also supported, but conditional on the brokers having `delete.topic.enable=true`.

### Offset management

Allow storing and fetching an arbitrary number of offsets in Kafka itself without having to worry about offset coordinators. With this ability a fully functioning batch processing pipeline can be built around Kafka with just a few bash scripts.

## Building

You need to have maven 3.x to build the tools. Run `mvn clean package -P shade` to produce a shaded jar under the target directory.

Alternatively, if you have GNU make installed, run `make` to build the shaded jars and bundle them into executable scripts under the `bin` directory.

The shaded jar is useful if you want to distribute a single binary, although a JRE >= 6.x still needs to be present.

Finally, Maven can generate an RPM by running `mvn package`. Once installed, that will create its own `kafka8-tools` wrapper script.

## Running

You can run the shaded jar with `java -jar target/kafka8-tools.jar`, or the shaded scripts with `bin/kafka8-tools.sh`. If you have installed the RPM instead, just run `kafka8-tools.

To see a full list of subcommands and arguments run `kafka8-tools --help`, or for subcommand help run `kafka8-tools <command> --help`.

## Examples

These examples assume a ZooKeeper instance on port 2181, and three Kafka brokers (1, 2 and 3) at ports 9091, 9092 and 9093 respectively.

1.  Create a topic with 2 partitions, a replication factor of 3 and a minimum in sync replica set (ISR) of 2:

    ```bash
    kafka8-tools topics -s localhost:2181 -t topic1 -p 2 -r 3 -m 2 --create
    ```

2.  Verify the configuration and status of the topic:

    ```bash
    kafka8-tools topics -s localhost:2181 -t topic1 --info --json
    ```
    ```json
    {"topic":"topic1","details":{"1":{"leader":3,"replicas":[1,2,3],"isr":[1,2,3]},"0":{"leader":2,"replicas":[1,2,3],"isr":[1,2,3]}},"deleted":false,"configuration":{"min.insync.replicas":"2"},"partitions":2,"replication":3}
    ```

4.  Push space-delimited key-value pairs to the first partition of our topic, with an ISR ack-policy:

    ```bash
    echo -e "k1 v1\nk2 v2\nk3 v3" | kafka8-tools push -s localhost:9091 -t topic1 -p 0 --acks -1 --keys --values --field-separator ' '
    ```

    Note how even though broker 2 (port 9092) was the partition leader, no error was raised, as the leader was automatically discovered before sending the data through.

5.  Push value-only data to our second partition, followed by some key-only data, with a leader-only ack-policy:

    ```bash
    echo -e "v4\nv5" | kafka8-tools push -s localhost:9092 -t topic1 -p 1 --acks 1 --values
    ```

    ```bash
    echo -e "k7\nk8" | kafka8-tools push -s localhost:9093 -t topic1 -p 1 --acks 1 --keys
    ```

    Again note how we can choose any one of the replicas as our contact endpoint.

6.  Pull the data out of our topic using JSON as the output format. Fetch keys and values, along with the their offsets in the partition:

    ```bash
    for p in 0 1; do
      kafka8-tools pull -s localhost:9091 -t topic1 -p $p -o $(kafka8-tools pull -s localhost:9091 -t topic1 -p $p --earliest-offset) --keys --values --offsets --json
    done
    ```

    ```json
    {"o":0,"k":"k1","v":"v1"}
    {"o":1,"k":"k2","v":"v2"}
    {"o":2,"k":"k3","v":"v3"}
    {"o":0,"k":null,"v":"v4"}
    {"o":1,"k":null,"v":"v5"}
    {"o":2,"k":"k7","v":null}
    {"o":3,"k":"k8","v":null}
    ```

    Notice how we fetched the earliest logical offset available with an additional call to kafka8-tools. Also the broker we chose did not make a difference in either case.

    For a full list of options when pulling data, including additional ways to determine offsets (e.g. last offset, or offset before some time), or ways to change the output schema, run `kafka8-tools pull --help`.

7.  We can take advantage of Kafka's offset storage to keep track of our consumer offsets:

    ```bash
    kafka8-tools offsets -s localhost:9091 -t topic1 -p 0 -g group1 --commit 2
    kafka8-tools offsets -s localhost:9091 -t topic1 -p 1 -g group1 --commit 3
    ```

    Note that the first time you run this command you may get an error like the following, in which case you have to rerun it:

    ```
    fatal kafka.common.ConsumerCoordinatorNotAvailableException: null
    ```

    This is because the cluster has not finished electing a coordinator for offset storage. Future version might allow retrying on this condition automatically.

    The full list of topics should now include the special topic used for offset storage, e.g.:

    ```bash
    kafka8-tools topics -s localhost:2181
    ```
    ```
    __consumer_offsets
    topic1
    ```


8.  Fetch the offset we last committed for our group:

    ```bash
    kafka8-tools offsets -s localhost:9092 -t topic1 -p 0 -g group1
    ```
    ```
    2
    ```
    ```bash
    kafka8-tools offsets -s localhost:9092 -t topic1 -p 1 -g group1
    ```
    ```
    3
    ```

9.  Delete the topic:

    ```bash
    kafka8-tools topics -s localhost:2181 -t topic1 --delete
    ```

    The topic will be marked for deletion in ZooKeeper but it will only take affect as soon as the brokers involved are restarted with `delete.topic.enable=true`.

