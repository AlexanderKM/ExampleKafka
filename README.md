# ExampleKafka

An example Kafka producer and consumer

## Requirements

- Java 8
- Kafka 1.0.0
- A running kafka cluster to produce to and consume from

## Setup

To run the consumer or producer, a properties file must be created for both under `src/main/resources`.

### Consumer Configs

See [kafka consumer configs](https://kafka.apache.org/documentation/#newconsumerconfigs) for more detail.

Create a `consumer.properties` file under `src/main/resources` and specify:

| Property | Suggested Value | Description |
| ------ | ----- | ----- |
| `bootstrap.servers` | csv of 3 brokers `ip:port,ip:port,ip:port` | list of host/port pairs to use for establishing the initial connection to the Kafka cluster |
| `group.id` | n/a | a unique name for the consumer group for this consumer |
| `enable.auto.commit` | false | set to false for this project- if true the consumer's offset will be periodically committed in the background. |
| `key.deserializer` | org.apache.kafka.common.serialization.StringDeserializer |Deserializer class for key that implements the `org.apache.kafka.common.serialization.Deserializer` interface. |
| `value.deserializer` |  org.apache.kafka.common.serialization.StringDeserializer |Deserializer class for value that implements the `org.apache.kafka.common.serialization.Deserializer` interface. |
| `topic` | n/a | A single topic for the consumer to consume from |

### Producer Configs

See [kafka producer configs](https://kafka.apache.org/documentation/#producerconfigs) for more detail.

Create a `producer.properties` file under `src/main/resources` and specify:

| Property | Suggested Value | Description |
| ------ | ----- | ----- |
| `bootstrap.servers` | csv of 3 brokers `ip:port,ip:port,ip:port` | list of host/port pairs to use for establishing the initial connection to the Kafka cluster |
| `acks` | all |Example values: 0, 1, all. The number of acknowledgments the producer requires the leader to have received before considering a request complete. |
| `retries` | 3 |Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error |
| `batch.size` | 16384 | The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. This helps performance on both the client and the server. This configuration controls the default batch size in bytes. |
| `linger.ms` | 1 |  rather than immediately sending out a record the producer will wait for up to the given delay to allow other records to be sent so that the sends can be batched together |
| `buffer.memory` | 33554432 | The total bytes of memory the producer can use to buffer records waiting to be sent to the server |
| `key.deserializer` | org.apache.kafka.common.serialization.StringDeserializer |Deserializer class for key that implements the `org.apache.kafka.common.serialization.Deserializer` interface. |
| `value.deserializer` |  org.apache.kafka.common.serialization.StringDeserializer |Deserializer class for value that implements the `org.apache.kafka.common.serialization.Deserializer` interface. |
| `topic` | n/a | A single topic for the producer to produce to |


