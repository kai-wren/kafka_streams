# Table lookup example

1. create input topic for user purchases
    ```
    ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases
    ```

2. create table of user information - log compacted for optimisation
    ```
    ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic user-table --config cleanup.policy=compact
    ```

3. create out topic for user purchases enriched with user data (left join)
    ```
    ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-enriched
    ```
4. start a consumer on the output topic
    ```
    ./kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic user-purchases-enriched \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
    ```