# Seatunnel

## Environment Preparation

set alias in bashrc

```shell
echo "alias dseatunnel='cd /opt/poc-allin1/docker/compute/seatunnel && docker compose -f docker-compose.yml '" >> ~/.bashrc

source ~/.bashrc
```

## Submit Job

### Approach.1. Submit Job to Seatunnel Cluster

```shell
# submit job :
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run --name seatunnel_client \
    --network seatunnel_seatunnel_network \
    -e ST_DOCKER_MEMBER_LIST=172.16.0.2:5801 \
    -v ./seatunnel/job:/opt/seatunnel/job \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh -c config/v2.batch.config.template

docker run --name seatunnel_client \
    --network seatunnel_seatunnel_network \
    -e ST_DOCKER_MEMBER_LIST=192.168.138.15:5801 \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh  -c config/v2.batch.config.template
    
docker run --name seatunnel_client \
    --network seatunnel_seatunnel_network \
    -e ST_DOCKER_MEMBER_LIST=seatunnel-master:5801 \
    -v ./seatunnel/job:/opt/seatunnel/job \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh -c config/v2.batch.config.template

# list job
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run --name seatunnel_client \
    --network seatunnel_seatunnel_network \
    -e ST_DOCKER_MEMBER_LIST=172.16.0.2:5801 \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh  -l

# submit job to cluster
docker run --name seatunnel_client \
    --network seatunnel_seatunnel_network \
    -e ST_DOCKER_MEMBER_LIST=172.16.0.2:5801 \
    -v ./seatunnel/job:/opt/seatunnel/job \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh -c /opt/seatunnel/job/sqlserver2paimon.stream.conf
```

```shell
export network="seatunnel_seatunnel_network"
export master_url="172.16.0.2:5801"

# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run --name seatunnel_client \
    --network $network \
    -e ST_DOCKER_MEMBER_LIST=$master_url \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh  -c config/v2.batch.config.template


# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run --name seatunnel_client \
    --network $network \
    -e ST_DOCKER_MEMBER_LIST=$master_url \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh  -l
```

### Approach.2. Submit Job to Seatunnel Client

With running seatunnel-client container, to Access the Seatunnel CLI

```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/xxx
```

E.g.:
```shell
docker exec -it seatunnel-client bin/seatunnel.sh -c job/sftpcsv2console.batch.conf
```

### Approach.3. Submit Job within Container

Entering Container 

```shell
docker exec -it seatunnel-client bash

bin/seatunnel.sh -c job/sqlserver2paimon.stream.conf
```

### Approach.4. Submit Job with RESTful API

SeaTunnel has a monitoring API that can be used to query status and statistics of running job, as well as recent completed job. The monitoring API is a RESTful API that accepts HTTP requests and responds with JSON data.

Enable HTTPS in seatunnel.yaml:

```
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

```shell
POST /submit-job
```

# Flink SQL Client - Query Paimon

```sql
-- CATALOG
CREATE CATALOG paimon_catalog WITH (
  'type'='paimon',
  'warehouse'='s3a://warehouse/paimon/seatunnel/',
  's3.endpoint'='http://192.168.138.15:9000',
  's3.access-key'='minioadmin',
  's3.secret-key'='minioadmin',
  's3.path.style.access'='true'
);
       
USE CATALOG paimon_catalog;

USE paimon;

select count(*) from order_protobuf_format;
select * from order_protobuf_format;

-- org.apache.flink.table.api.TableException: Column 'id' is NOT NULL, however, a null value is being written into it. You can set job configuration 'table.exec.sink.not-null-enforcer'='DROP' to suppress this exception and drop such records silently.
set 'table.exec.sink.not-null-enforcer'='DROP';


SET 'execution.checkpointing.interval' = '5 s';

-- switch to streaming mode
SET 'execution.runtime-mode' = 'streaming';
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';

-- switch to batch mode
RESET 'execution.checkpointing.interval';
SET 'execution.runtime-mode' = 'batch';

SELECT id,order_id,supplier_id,item_id,qty FROM seatunnel_sqlserver_paimon_sink where id = 247048;
```

### Set Up Develop Env

If ./mvnw install -Dmaven.test.skip Fail, try this: `./mvnw install "-Dmaven.test.skip=true"`

https://github.com/apache/maven-mvnd/issues/854

```
If this is in PowerShell, then using double quotes might help: "-Dmaven.test.skip=true".
BTW, "-Dmaven.test.skip" should be equivalent.
```