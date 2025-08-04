# Seatunnel

Seatunnel is a distributed data integration tool that provides a unified platform for batch and stream data processing. It is designed to be easy to use, flexible, and scalable, making it suitable for a wide range of data integration scenarios.
Official website: [https://seatunnel.apache.org/](https://seatunnel.apache.org/)

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

# UDF

```
bin/seatunnel.sh -m local -c job/fake2console.sql-udf.batch.conf
```

|Issue| Missing Jar                                                                            |
|-|----------------------------------------------------------------------------------------|
|java.lang.ClassNotFoundException: org.apache.seatunnel.transform.sql.zeta.ZetaUDF| mvn dependencies 2.3.x jar version not matching or miss lib/seatunnel-transform-v2.jar |

```
wget -P lib https://repo1.maven.org/maven2/org/apache/seatunnel/seatunnel-transforms-v2/2.3.11/seatunnel-transforms-v2-2.3.11.jar
```
