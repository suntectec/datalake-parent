# Flink

## Environment Preparation

set alias in bashrc

```shell
echo "alias dflink='cd /opt/paimon-poc/compute/flink && docker compose -f docker-compose.yml '" >> ~/.bashrc

source ~/.bashrc
```

## Submit Job

### Approach.1. Submit Job to Flink Cluster

```shell
docker exec -it flink-jobmanager bin/sql-client.sh embedded -f /opt/flink/jobs/sqlserver2paimon.s3.sql
```

### Approach.2. Submit Job to Flink SQL Client

With running flink-client container, to Access the SQL CLI

```shell
docker exec -it flink-client bin/sql-client.sh embedded -f /opt/flink/jobs/sqlserver2paimon.s3.sql
```

### Approach.3. Submit Job within Container

Entering Container

```shell
docker exec -it flink-jobmanager bin/sql-client.sh embedded

docker exec -it flink-client bin/sql-client.sh embedded
```

# Flink SQL Client Query and Options

```sql
CREATE CATALOG paimon_catalog WITH (
  'type'='paimon',
  'warehouse'='s3://warehouse/paimon/',
  's3.endpoint'='http://192.168.138.15:9000',
  's3.access-key'='minioadmin',
  's3.secret-key'='minioadmin',
  's3.path.style.access'='true'
);
       
USE CATALOG paimon_catalog;

USE inventory;

select count(*) from orders;
select * from orders;

-- required set before submit insert job, otherwise data not observe
-- execution.checkpointing.interval: default - none, The base interval setting. To enable checkpointing, you need to set this value larger than 0.
SET 'execution.checkpointing.interval' = '10 s';

-- switch to streaming mode
SET 'execution.runtime-mode' = 'streaming';
-- switch to batch mode
SET 'execution.runtime-mode' = 'batch';
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'TABLE';
-- SET 'sql-client.execution.result-mode' = 'CHANGELOG';
SET 'sql-client.execution.result-mode' = 'tableau';
-- or RESET
RESET 'execution.checkpointing.interval';

-- track the changes of table and calculate the count interval statistics
SELECT `status`, SUM(qty) AS qty_total FROM orders GROUP BY `status`;
```