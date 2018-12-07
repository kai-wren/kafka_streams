#Download and Install Confluent Platform

* Go to the https://www.confluent.io/download/ page and choose Confluent Open Source.

* Provide your name and email and select Download.

* Decompress the file. 
You should have these directories:

Folder|Description
---|---
/bin/ | Driver scripts for starting and stopping services
/etc/ | Configuration files
/lib/ | Systemd services
/logs/ | Log files
/share/ | Jars and licenses
/src/ | Source files that require a platform-dependent build

* Install the Confluent Hub client. This is used in the next step to install the free and open source kafka-source-datagen connector.
   
    * Tap the Confluent repository for the Confluent Hub client.
        ```
        brew tap confluentinc/homebrew-confluent-hub-client
        ```

    * Install the Confluent Hub client.
        ```
        brew cask install confluent-hub-client
        ```
    
    * Optional: Verify your installation by typing confluent-hub in your terminal.
        ```
        confluent-hub
        ```
    
* Install the source connector *kafka-connect-datagen* from Confluent Hub.
```
<path-to-confluent>/bin/confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.1.0
```

* Start Confluent Platform using the Confluent CLI. 
This command will start all of the Confluent Platform components, including Kafka, ZooKeeper, Schema Registry, HTTP REST Proxy for Apache Kafka, Kafka Connect, and KSQL. 
``` 
<path-to-confluent>/bin/confluent start
```

Your output should resemble:
```
Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
Starting kafka-rest
kafka-rest is [UP]
Starting connect
connect is [UP]
Starting ksql-server
ksql-server is [UP]
```

