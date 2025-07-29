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

......