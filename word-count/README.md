# WordCount End-to-End Example

1. create input topic with two partitions
    ```
    ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-input
    ```

2. create output topic
    ```
    ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-output
    ```

3. launch a Kafka consumer
    ```
    ./kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic word-count-output \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
    ```

4. launch the streams application

5. then produce data to it
    ```
    ./kafka-console-producer --broker-list localhost:9092 --topic word-count-input
    ```

6. list all topics that we have in Kafka (so we can observe the internal topics)
    ```
    ./kafka-topics.sh --list --zookeeper localhost:2181
    ```
