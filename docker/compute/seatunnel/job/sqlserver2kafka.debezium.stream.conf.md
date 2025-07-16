### Submit Job

```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/sqlserver2kafka.debezium.stream.conf
```

### Kafka Consumer

default from `--offset latest`

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic Debezium.INV.orders"
```
or`
```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic Debezium.INV.orders --offset latest --partition 0"
```

`--from-beginning`

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic Debezium.INV.orders --from-beginning"
```

### Kafka messages:

```
{
	"before": null,
	"after": {
		"id": 2479551,
		"order_id": "56dc6d06-f2fa-4dbd-8c94-7e43f8b6b9c9",
		"supplier_id": 2750,
		"item_id": 79,
		"status": "created",
		"qty": 1300,
		"net_price": 3100,
		"issued_at": "2025-07-10T15:57:17.467",
		"completed_at": "2025-07-10T15:57:17.467",
		"spec": null,
		"created_at": "2025-07-10T15:57:17.467",
		"updated_at": "2025-07-10T15:57:17.467"
	},
	"op": "c"
}
```