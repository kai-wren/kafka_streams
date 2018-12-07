#Hands-On Exercise: KSQL
###Create Kafka Topics
In this step Kafka topics are created in Confluent Platform by using the Kafka CLI.
   
1. Run this command to create a topic named users.
   ```
   ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic users
   ```
   Your output should resemble:
   ```
   Created topic "users".
   ```
2. Run this command to create a topic named pageviews.
   ```
   ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic pageviews
   ```
   Your output should resemble:
   ```
   Created topic "pageviews".
   ```
   
###Generate Sample Data
   In this step, you use Kafka Connect to run a demo source connector called kafka-connect-datagen that creates sample data for the Kafka topics pageviews and users.
   
1. Run one instance of the kafka-connect-datagen connector to produce Kafka data to the pageviews topic in JSON format.
   ```
   wget https://github.com/confluentinc/kafka-connect-datagen/raw/master/config/connector_pageviews_cos.config
   
   ./confluent config datagen-pageviews -d ./connector_pageviews_cos.config
   ```
2. Run another instance of the kafka-connect-datagen connector to produce Kafka data to the users topic in JSON format.
   ```
   wget https://github.com/confluentinc/kafka-connect-datagen/raw/master/config/connector_users_cos.config
   
   ./confluent config datagen-users -d ./connector_users_cos.config
   ```
###Stream and Table using KSQL
   In this step KSQL queries are run on the pageviews and users topics that were created in the previous step.
   
1. Create a stream (pageviews) from the Kafka topic pageviews, specifying the value_format of JSON.
   ```
   ksql> CREATE STREAM pageviews (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH (KAFKA_TOPIC='pageviews', VALUE_FORMAT='JSON');
   ```
   Tip: Enter the *SHOW STREAMS;* command to view your streams. For example:
   ```
    Stream Name      | Kafka Topic      | Format
   -------------------------------------------------
    PAGEVIEWS        | pageviews        | JSON
   -------------------------------------------------
   ```
   
2. Create a table (users) with several columns from the Kafka topic users, with the value_format of JSON.
   ```
   ksql> CREATE TABLE users (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR, \interests array<VARCHAR>, contact_info map<VARCHAR, VARCHAR>) WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON', KEY = 'userid');
   ```
   Tip: Enter the SHOW TABLES; query to view your tables.
   ```
    Table Name        | Kafka Topic       | Format    | Windowed
   --------------------------------------------------------------
    USERS             | users             | JSON      | false
   --------------------------------------------------------------
   ```
###Write Queries
   These examples write queries using KSQL. The following KSQL commands are run from the KSQL CLI. Enter these commands in your terminal and press Enter.
   
1. Add the custom query property earliest for the auto.offset.reset parameter. This instructs KSQL queries to read all available topic data from the beginning. This configuration is used for each subsequent query. For more information, see the KSQL Configuration Parameter Reference.
   ```
   ksql> SET 'auto.offset.reset'='earliest';
   ```
   Your output should resemble:
   ```
   Successfully changed local property 'auto.offset.reset' from 'null' to 'earliest'
   ```
   
2. Create a query that returns data from a stream with the results limited to three rows.
   ```
   ksql> SELECT pageid FROM pageviews LIMIT 3;
   ```
   Your output should resemble:
   ```
   Page_45
   Page_38
   Page_11
   LIMIT reached for the partition.
   Query terminated
   ```
   
3. Create a persistent query that filters for female users. The results from this query are written to the Kafka PAGEVIEWS_FEMALE topic. This query enriches the pageviews STREAM by doing a LEFT JOIN with the users TABLE on the user ID, where a condition (gender = 'FEMALE') is met.
   ```
   ksql> CREATE STREAM pageviews_female AS SELECT users.userid AS userid, pageid, regionid, gender FROM pageviews LEFT JOIN users ON pageviews.userid = users.userid WHERE gender = 'FEMALE';
   ```
   Your output should resemble:
   ```
    Message
   ----------------------------
    Stream created and running
   ----------------------------
   ```
   
4. Create a persistent query where a condition (regionid) is met, using LIKE. Results from this query are written to a Kafka topic named pageviews_enriched_r8_r9.
   ```
   ksql> CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='JSON') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';
   ```
   Your output should resemble:
   ```
    Message
   ----------------------------
    Stream created and running
   ----------------------------
   ```
5. Create a persistent query that counts the pageviews for each region and gender combination in a tumbling window of 30 seconds when the count is greater than 1. Because the procedure is grouping and counting, the result is now a table, rather than a stream. Results from this query are written to a Kafka topic called PAGEVIEWS_REGIONS.
   ```
   ksql> CREATE TABLE pageviews_regions AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_female WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;
   ```
   Your output should resemble:
   ```
    Message
   ---------------------------
    Table created and running
   ---------------------------
   ```