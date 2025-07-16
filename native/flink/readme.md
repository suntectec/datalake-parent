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

### Run SeaTunnel Engine Job

E.g.: ./cli.sh run apache-seatunnel-2.3.11/config/v2.batch.config.template

```bash
./cli.sh run <job_file>
```

### Stop SeaTunnel Engine Server Node

```bash
./cli.sh stop
```