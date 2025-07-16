### Submit Job

```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/sqlserver2kafka.stream.conf
```

### Kafka Consumer

default from `--offset latest`

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic INV.orders"
```
or
```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic INV.orders --offset latest --partition 0"
```

`--from-beginning`

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic INV.orders --from-beginning"
```

### Kafka messages:

```
{
	"id": 2481722,
	"order_id": "3b648d15-7edf-4342-af22-3556bef2e9e4",
	"supplier_id": 884,
	"item_id": 64,
	"status": "created",
	"qty": 500,
	"net_price": 4640,
	"issued_at": "2025-07-10T16:15:56.103",
	"completed_at": "2025-07-10T16:15:56.103",
	"spec": null,
	"created_at": "2025-07-10T16:15:56.103",
	"updated_at": "2025-07-10T16:15:56.103"
}
```