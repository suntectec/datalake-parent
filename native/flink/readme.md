# Flink

Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale.
Official website: [https://flink.apache.org/](https://flink.apache.org/)

## Usage

### Installation

```bash
./cli.sh install
```

### Start SeaTunnel Engine Server Node

```bash
./cli.sh start
```

### Run Job

E.g.: ./cli.sh run apache-seatunnel-2.3.11/config/v2.batch.config.template

```bash
./cli.sh run <job_file>
```

### Stop SeaTunnel Engine Server Node

```bash
./cli.sh stop
```

# Flink SQL Gateway

```
./bin/start-cluster.sh
```

Starting the SQL Gateway

```
./bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost
```

```
curl http://192.168.138.15:8083/v1/info
```

Running SQL Queries

```
curl --request POST http://192.168.138.15:8083/v1/sessions
```

# Paimon-Flink-Action - CDC Ingestion with Action Jars

[Paimon - CDC Ingestion - Mysql CDC](https://paimon.apache.org/docs/1.2/cdc-ingestion/mysql-cdc/)

Prepare CDC Bundled Jar #
Download CDC Bundled Jar and put them under <FLINK_HOME>/lib/.

Version	Bundled Jar
3.1.x (Only cdc 3.1+ is supported.)
flink-sql-connector-mysql-cdc-3.1.x.jar
mysql-connector-java-8.0.27.jar

then required restart cluster

Issue: paimon-flink-action occurs lack a list of multi sub-dependencies of flink-sql-connector-mysql-cdc-*.jar
高版本 cdc 依赖，存在 kafka 依赖包重定向问题 - Resolved by: Paimon-Flink-Action now only supports cdc 3.1.x+ version

and higher version of flink-sql-connector-mysql-cdc-*.jar will run lack a list of dependencies in flink-sql-connector-*-cdc fat jar which needs build higher version flink-sql-connector-*-cdc fat jar by yourself and 
redirect kafka connector api or kafka client dependencies to flink-sql-connector-kafka-*.jar (so not advised to use higher version of flink-sql-connector-*-cdc fat jar)

Synchronizing Databases:

```shell
# MySQL-Paimon Database Sync: inventory - Paimon File
./bin/flink run \
    ./lib/plugin/paimon-flink-action-1.2.0.jar \
    mysql_sync_database \
    --warehouse /tmp/warehouse1  \
    --database inventory \
    --mysql_conf hostname=192.168.138.15 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=inventory \
    --mysql_conf server-time-zone=Asia/Macau \
    --table-conf bucket=1
    
# MySQL-Paimon Database Sync: inventory - Paimon S3
./bin/flink run -d \
    ./lib/plugin/paimon-flink-action-1.2.0.jar \
    mysql_sync_database \
    --warehouse s3://lakehouse/paimon \
    --database inventory \
    --mysql_conf hostname=192.168.138.15 \
    --mysql_conf port=3306 \
    --mysql_conf server-time-zone=Asia/Macau \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=inventory \
    --catalog_conf s3.endpoint=http://192.168.138.15:9000 \
    --catalog_conf s3.path.style.access=true \
    --catalog_conf s3.access-key=minioadmin \
    --catalog_conf s3.secret-key=minioadmin \
    --table_conf bucket=1 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=1
```

Result:

```
Data.Eng@dev-ds-trm01:~$ ll /tmp/warehouse1/inventory.db/orders/
total 24
drwxrwxr-x 6 Data.Eng Data.Eng 4096 Aug  6 11:02 ./
drwxrwxr-x 9 Data.Eng Data.Eng 4096 Aug  6 10:45 ../
drwxrwxr-x 2 Data.Eng Data.Eng 4096 Aug  6 11:02 bucket-0/
drwxrwxr-x 2 Data.Eng Data.Eng 4096 Aug  6 11:05 manifest/
drwxrwxr-x 2 Data.Eng Data.Eng 4096 Aug  6 10:45 schema/
drwxrwxr-x 2 Data.Eng Data.Eng 4096 Aug  6 11:05 snapshot/
```

[Paimon - CDC Ingestion - Kafka CDC](https://paimon.apache.org/docs/1.2/cdc-ingestion/kafka-cdc/)

Prepare Kafka Bundled Jar #
flink-sql-connector-kafka-*.jar

Issue: ClassNotFoundException: org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
- Resolved by: after adding flink-sql-connector-* fat jar package, then required restart flink cluster 

Synchronizing Databases:

```shell
# Kafka-Paimon Table Sync: inventory.INV.orders - Paimon File
./bin/flink run \
    ./lib/plugin/paimon-flink-action-1.2.0.jar \
    kafka_sync_table \
    --warehouse /tmp/warehouse2  \
    --database inventory \
    --table INV.orders \
    --primary_keys id \
    --kafka_conf properties.bootstrap.servers=192.168.138.15:9092 \
    --kafka_conf topic=SqlServer.Orders \
    --kafka_conf properties.group.id=paimon-group \
    --kafka_conf value.format=debezium-json \
    --kafka_conf scan.startup.mode=earliest-offset \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4

# Kafka-Paimon Database Sync: inventory - Paimon File
./bin/flink run \
    ./lib/plugin/paimon-flink-action-1.2.0.jar \
    kafka_sync_database \
    --warehouse /tmp/warehouse2  \
    --database inventory \
    --primary_keys id \
    --kafka_conf properties.bootstrap.servers=192.168.138.15:9092 \
    --kafka_conf topic=SqlServer.Orders \
    --kafka_conf properties.group.id=paimon-group \
    --kafka_conf value.format=debezium-json \
    --kafka_conf scan.startup.mode=earliest-offset \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4

# Synchronization from one Kafka topic to Paimon database.
./bin/flink run \
    lib/plugin/paimon-flink-action-1.2.0.jar \
    kafka_sync_database \
    --warehouse s3://lakehouse/paimon \
    --database ods \
    --table_prefix ods_inventory_db_ \
    --kafka_conf properties.bootstrap.servers=192.168.138.15:9092 \
    --kafka_conf topic=SqlServer.Orders \
    --kafka_conf properties.group.id=paimon-group \
    --kafka_conf value.format=debezium-json \
    --kafka_conf scan.startup.mode=earliest-offset \
    --catalog_conf s3.endpoint=http://192.168.138.15:9000 \
    --catalog_conf s3.path.style.access=true \
    --catalog_conf s3.access-key=minioadmin \
    --catalog_conf s3.secret-key=minioadmin \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
    
# Synchronization from multiple Kafka topics to Paimon database.
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-1.2.0.jar \
    kafka_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=order\;logistic_order\;user \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=canal-json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```