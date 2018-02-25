
# Kafka Restful Sink Connector
This connector is built to consume messages from a kafka topic and post them to an endpoint which should be included in the kafka message.
## Running
To test the connector in standalone mode use the following command:
```$KAFKA_HOME/bin/connect-standalone.sh config/connect-standalone.properties config/kafka-restful-connector.properties```

## Configuration
The file [kafka-restful-connector.properties](src/examples/kafka-restful-connector.properties) contains a sample of the properties the application needs in order to run.
 
Property|Description|Default Value
---|---|---
name|This is a unique name for the connector|
connector.class|This is the Connector class|
message.class|This is the class for the deserialization of the message|
entity.class|This is the class of the HTTP entity for the POST request|
entity.serializer.class|This is the class of the serializer for the entity| 
tasks.max|This is the max number of tasks|
bulk.size|The max number of records to send at any one time|
topics|The topics to consume from as a comma separated list|
