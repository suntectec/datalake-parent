### Submit Job

```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/sqlserver2kafka.compatible_debezium.stream.conf
```

### Kafka Consumer

default from `--offset latest`

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic CompatibleDebezium.INV.orders"
```
or
```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic CompatibleDebezium.INV.orders --offset latest --partition 0"
```

`--from-beginning`

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic CompatibleDebezium.INV.orders --from-beginning"
```

### Debezium provides a unified format for changelog, here is a simple example for an update operation captured from a MySQL products table:

```
{
    "before": {
        "id": 111,
        "name": "scooter",
        "description": "Big 2-wheel scooter ",
        "weight": 5.18
    },
    "after": {
        "id": 111,
        "name": "scooter",
        "description": "Big 2-wheel scooter ",
        "weight": 5.17
    },
    "source": {
        "version": "1.1.1.Final",
        "connector": "mysql",
        "name": "dbserver1",
        "ts_ms": 1589362330000,
        "snapshot": "false",
        "db": "inventory",
        "table": "products",
        "server_id": 223344,
        "gtid": null,
        "file": "mysql-bin.000003",
        "pos": 2090,
        "row": 0,
        "thread": 2,
        "query": null
    },
    "op": "u",
    "ts_ms": 1589362330904,
    "transaction": null
}
```

**_ts_ms_**:
Optional field that displays the time at which the connector processed the event. The time is based on the system clock in the JVM running the Kafka Connect task.
In the source object, ts_ms indicates the time that the change was made in the database. By comparing the value for payload.source.ts_ms with the value for payload.ts_ms, you can determine the lag between the source database update and {prodname}.

Note: please refer to [Debezium documentation](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#data-change-events) about the meaning of each fields.

refer to [Seatunnel - Debezium Format](https://seatunnel.apache.org/docs/2.3.11/connector-v2/formats/debezium-json/)