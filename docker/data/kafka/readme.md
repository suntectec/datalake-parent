[docker-compose.yml](docker-compose.yml)

解决了容器内外网访问的问题，指定了 KAFKA_ADVERTISED_LISTENERS

**advertised.host.name**: DEPRECATED: only used when advertised.listeners or listeners are not set. Use advertised.listeners instead. Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may need to be different from the interface to which the broker binds. If this is not set, it will use the value for host.name if configured. Otherwise it will use the value returned from java.net.InetAddress.getCanonicalHostName().

**advertised.listeners**: Listeners to publish to ZooKeeper for clients to use, if different than the listeners config property. In IaaS environments, this may need to be different from the interface to which the broker binds. If this is not set, the value for listeners will be used. Unlike listeners it is not valid to advertise the 0.0.0.0 meta-address.

参考：[listeners与advertised.listeners的区别](https://blog.csdn.net/qq_39526294/article/details/124293954)

创建Topic

```
docker exec -it kafka \
bash -c "/bin/kafka-topics --create --bootstrap-server 192.168.138.15:9092 --replication-factor 1 --partitions 3 --topic test-topic"
```

生产消息

```
docker exec -it kafka \
bash -c "/bin/kafka-console-producer --bootstrap-server 192.168.138.15:9092 --topic test-topic"
```

消费消息

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic test-topic --from-beginning"
```

[docker-compose-template.yml](docker-compose-template.yml)[docker-compose.yml](docker-compose-bitnami.yml)

PS: 存在容器内容内网可访问，外网无法访问的问题，需要重新设定 KAFKA_ADVERTISED_LISTENERS

创建Topic

```
docker exec -it kafka \
bash -c "/bin/kafka-topics --create --bootstrap-server 192.168.138.15:9092 --replication-factor 1 --partitions 3 --topic test-topic"
```

生产消息

```
docker exec -it kafka \
bash -c "/bin/kafka-console-producer --bootstrap-server 192.168.138.15:9092 --topic test-topic"
```

消费消息

```
docker exec -it kafka \
bash -c "/bin/kafka-console-consumer --bootstrap-server 192.168.138.15:9092 --topic test-topic --from-beginning"
```


[docker-compose-bitnami.yml](docker-compose-bitnami.yml)


创建Topic

```
docker exec -it kafka \
bash -c "kafka-topics.sh --create --bootstrap-server 192.168.138.15:9092 --replication-factor 1 --partitions 3 --topic test-topic"
```

生产消息

```
docker exec -it kafka \
bash -c "kafka-console-producer.sh --bootstrap-server 192.168.138.15:9092 --topic test-topic"
```

消费消息

```
docker exec -it kafka \
bash -c "kafka-console-consumer.sh --bootstrap-server 192.168.138.15:9092 --topic test-topic --from-beginning"
```