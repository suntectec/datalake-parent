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

在使用 2.3.2 版本测试的过程中，tar 解压安装包后，在 lib 目录下并没用 seatunnel-transform-v2.jar，但这在我们的 job config 中使用到了 transform，
所以需要我们先手动 wget 下载这个 2.3.2 版本对应的 seatunnel-transform-v2.jar
（使用 bin/install-plugin.sh 只会读 config/plugin_config 自动下载 connectors 和 lib/seatunnel-hadoop3-3.1.4-uber-2.3.2-optional.jar）

### 为了使用 tree 命令记录这次的操作文件，tree命令以树形结构显示文件目录结构

安装 tree 命令：
```
sudo apt install tree
```

使用命令：
```bash
tree
```
PS：tree -L 1 、tree -L 2 可以进行文件目录分级

```
.
├── bin
│   ├── install-plugin.sh
│   ├── seatunnel-cluster.sh
│   ├── seatunnel.sh
│   ├── start-seatunnel-flink-13-connector-v2.sh
│   ├── start-seatunnel-flink-15-connector-v2.sh
│   ├── start-seatunnel-spark-2-connector-v2.sh
│   ├── start-seatunnel-spark-3-connector-v2.sh
│   └── stop-seatunnel-cluster.sh
├── config
│   ├── hazelcast-client.yaml
│   ├── hazelcast.yaml
│   ├── jvm_client_options
│   ├── jvm_options
│   ├── log4j2_client.properties
│   ├── log4j2.properties
│   ├── plugin_config
│   ├── seatunnel-env.sh
│   ├── seatunnel.yaml
│   ├── v2.batch.config.template
│   └── v2.streaming.conf.template
├── connectors
│   ├── plugin-mapping.properties
│   └── seatunnel
│       ├── connector-amazondynamodb-2.3.2.jar
│       ├── connector-assert-2.3.2.jar
│       ├── connector-cassandra-2.3.2.jar
│       ├── connector-cdc-mysql-2.3.2.jar
│       ├── connector-cdc-sqlserver-2.3.2.jar
│       ├── connector-clickhouse-2.3.2.jar
│       ├── connector-console-2.3.2.jar
│       ├── connector-datahub-2.3.2.jar
│       ├── connector-dingtalk-2.3.2.jar
│       ├── connector-doris-2.3.2.jar
│       ├── connector-elasticsearch-2.3.2.jar
│       ├── connector-email-2.3.2.jar
│       ├── connector-fake-2.3.2.jar
│       ├── connector-file-ftp-2.3.2.jar
│       ├── connector-file-hadoop-2.3.2.jar
│       ├── connector-file-local-2.3.2.jar
│       ├── connector-file-oss-2.3.2.jar
│       ├── connector-file-s3-2.3.2.jar
│       └── connector-google-sheets-2.3.2.jar
├── DISCLAIMER
├── job
│   ├── fake2console.sql-udf.stream.conf
│   └── fake2console.sql-udf.stream.conf.md
├── lib
│   ├── seatunnel-hadoop3-3.1.4-uber-2.3.2-optional.jar
│   ├── seatunnel-transforms-v2-2.3.2.jar
│   └── udf-1.0-SNAPSHOT.jar
├── LICENSE
├── licenses
│   ├── LICENSE-asm.txt
│   ├── LICENSE-avro.txt
│   ├── LICENSE-connons-math.txt
│   ├── LICENSE-javax-annootation-api.txt
│   ├── LICENSE-orc.txt
│   ├── LICENSE-parquet-format.txt
│   ├── LICENSE-parquet-mr.txt
│   ├── LICENSE-protobuf.txt
│   ├── LICENSE-scala.txt
│   ├── LICENSE-sjf4j.txt
│   ├── LICENSE-xz.txt
│   └── LICENSE-yetus.txt
├── mvnw
├── mvnw.cmd
├── NOTICE
├── plugins
│   └── README.md
├── README.md
├── repo1.maven.org
│   └── maven2
│       └── org
│           └── apache
│               └── seatunnel
│                   └── seatunnel-transforms-v2
│                       └── 2.3.2
│                           └── seatunnel-transforms-v2-2.3.2.jar
└── starter
    ├── logging
    │   ├── jcl-over-slf4j-1.7.25.jar
    │   ├── log4j-api-2.17.1.jar
    │   ├── log4j-core-2.17.1.jar
    │   ├── log4j-slf4j-impl-2.17.1.jar
    │   └── slf4j-api-1.7.25.jar
    ├── seatunnel-flink-13-starter.jar
    ├── seatunnel-flink-15-starter.jar
    ├── seatunnel-spark-2-starter.jar
    ├── seatunnel-spark-3-starter.jar
    └── seatunnel-starter.jar

17 directories, 74 files

```

另外，经测试，在 2.3.2 版本中 自定义函数 udf.jar 必须与 lib 目录同级，即不能再 lib 目录下再自建一个文件夹目录存放 udf
如 lib/udf/udf.jar 这种方式是不可行的，必须放在 lib 目录下直接存放 ，即 lib/udf.jar 文件

其他注意的地方，

1、maven dependencies jar 版本 2.3.2 应与 seatunnel 版本 2.3.2 一直

2、由于 SeaTunnel UDF 是基于JAVA @AutoService 的 SPI 开发，
UDF Project 打包后会出现文件 target/classes/META-INF/services/org.apache.seatunnel.transform.sql.zeta.ZetaUDF，内容为对应 UDF 类路径，org.example.ExampleUDF
