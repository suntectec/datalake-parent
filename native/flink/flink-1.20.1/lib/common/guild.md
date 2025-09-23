DataStream API - Hadoop Jar Package: only add plugins/s3-fs-hadoop/flink-s3-fs-hadoop

SQL Client - Hadoop Jar Package: mv plugins/s3-fs-hadoop/flink-s3-fs-hadoop and add flink-shaded-hadoop to lib/common

PS: flink-s3-fs-hadoop can only occur once, alternative one place in plugins/s3-fs-hadoop/ or in lib/common

```
Data.Eng@dev-ds-trm01:/opt/poc-allin1/native/flink/flink-1.20.1$ tree lib
lib
├── common
│   ├── flink-clients-1.20.1.jar
│   ├── flink-core-1.20.1.jar
│   ├── flink-java-1.20.1.jar
│   ├── flink-s3-fs-hadoop-1.20.1.jar
│   ├── flink-shaded-hadoop-3-3.1.1.7.2.9.0-173-9.0.jar
│   ├── flink-streaming-java-1.20.1.jar
│   ├── flink-table-planner-loader-1.20.1.jar
│   ├── flink-table-runtime-1.20.1.jar
│   └── realtime-common-1.0-SNAPSHOT.jar
├── flink-cep-1.20.1.jar
├── flink-connector-files-1.20.1.jar
├── flink-csv-1.20.1.jar
├── flink-dist-1.20.1.jar
├── flink-json-1.20.1.jar
├── flink-scala_2.12-1.20.1.jar
├── flink-table-api-java-uber-1.20.1.jar
├── flink-table-planner-loader-1.20.1.jar
├── flink-table-runtime-1.20.1.jar
├── log4j-1.2-api-2.17.1.jar
├── log4j-api-2.17.1.jar
├── log4j-core-2.17.1.jar
└── log4j-slf4j-impl-2.17.1.jar

1 directory, 22 files

Data.Eng@dev-ds-trm01:/opt/poc-allin1/native/flink/flink-1.20.1$ tree plugins
plugins
├── external-resource-gpu
│   ├── flink-external-resource-gpu-1.20.1.jar
│   ├── gpu-discovery-common.sh
│   └── nvidia-gpu-discovery.sh
├── metrics-datadog
│   └── flink-metrics-datadog-1.20.1.jar
├── metrics-graphite
│   └── flink-metrics-graphite-1.20.1.jar
├── metrics-influx
│   └── flink-metrics-influxdb-1.20.1.jar
├── metrics-jmx
│   └── flink-metrics-jmx-1.20.1.jar
├── metrics-prometheus
│   └── flink-metrics-prometheus-1.20.1.jar
├── metrics-slf4j
│   └── flink-metrics-slf4j-1.20.1.jar
├── metrics-statsd
│   └── flink-metrics-statsd-1.20.1.jar
└── README.txt

8 directories, 11 files

```