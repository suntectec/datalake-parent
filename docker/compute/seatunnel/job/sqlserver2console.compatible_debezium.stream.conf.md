### Submit Job

```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/sqlserver2console.compatible_debezium.stream.conf
```

### Console

```
docker logs -f seatunnel-worker-1 -n 0
```

```
docker logs -f seatunnel-worker-2 -n 0
```

### Options Configuration Debizium for SQL Server Connector properties

The {prodname} SQL Server connector has numerous configuration properties that you can use to achieve the right connector behavior for your application. Many properties have default values.

_E.g._:

tombstones.on.delete is designed for Kafka log compaction, which is a feature that allows Kafka to remove older messages for a key as long as the most recent message is retained. This is particularly useful for maintaining a compacted topic where only the latest state of each key is needed.

```properties
tombstones.on.delete = false
```
default: true

### **_Refer to_**: [tombstones.on.delete](https://github.com/debezium/debezium/blob/1.6/documentation/modules/ROOT/pages/connectors/sqlserver.adoc#sqlserver-property-include-schema-changes)

Controls whether a delete event is followed by a tombstone event.

true - a delete operation is represented by a delete event and a subsequent tombstone event.

false - only a delete event is emitted.

After a source record is deleted, emitting a tombstone event (the default behavior) allows Kafka to completely delete all events that pertain to the key of the deleted row in case {link-kafka-docs}/#compaction[log compaction] is enabled for the topic.

### **_Refer to_**: [Table 6. Descriptions of delete event value fields](https://github.com/debezium/debezium/blob/1.6/documentation/modules/ROOT/pages/connectors/sqlserver.adoc#sqlserver-property-include-schema-changes)

SQL Server connector events are designed to work with Kafka log compaction. Log compaction enables removal of some older messages as long as at least the most recent message for every key is kept. This lets Kafka reclaim storage space while ensuring that the topic contains a complete data set and can be used for reloading key-based state.

Tombstone events
When a row is deleted, the delete event value still works with log compaction, because Kafka can remove all earlier messages that have that same key. However, for Kafka to remove all messages that have that same key, the message value must be null. To make this possible, after {prodname}’s SQL Server connector emits a delete event, the connector emits a special tombstone event that has the same key but a null value.
