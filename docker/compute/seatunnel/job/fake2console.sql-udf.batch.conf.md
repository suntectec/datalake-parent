```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/fake2console.sql-udf.batch.conf
```

or RESTful API

```
curl --location 'http://192.168.138.15:8080/submit-job/upload' --form 'config_file=@"/opt/poc-allin1/docker/compute/seatunnel/job/fake2console.sql-udf.batch.conf"'
```

```
docker logs -f seatunnel-worker-1 -n 0
```

```
docker logs -f seatunnel-worker-2 -n 0
```

|Issue| Missing Jar                                                                            |
|-|----------------------------------------------------------------------------------------|
|java.lang.ClassNotFoundException: org.apache.seatunnel.transform.sql.zeta.ZetaUDF| mvn dependencies 2.3.x jar version not matching or miss lib/seatunnel-transform-v2.jar |

SeaTunnel seatunnel-master Docker Container Tree:

```
.
├── LICENSE
├── NOTICE
├── README.md
├── bin
│   ├── install-plugin.cmd
│   ├── install-plugin.sh
│   ├── seatunnel-cluster.cmd
│   ├── seatunnel-cluster.sh
│   ├── seatunnel-connector.cmd
│   ├── seatunnel-connector.sh
│   ├── seatunnel.cmd
│   ├── seatunnel.sh
│   ├── start-seatunnel-flink-13-connector-v2.cmd
│   ├── start-seatunnel-flink-13-connector-v2.sh
│   ├── start-seatunnel-flink-15-connector-v2.cmd
│   ├── start-seatunnel-flink-15-connector-v2.sh
│   ├── start-seatunnel-spark-2-connector-v2.cmd
│   ├── start-seatunnel-spark-2-connector-v2.sh
│   ├── start-seatunnel-spark-3-connector-v2.cmd
│   ├── start-seatunnel-spark-3-connector-v2.sh
│   ├── stop-seatunnel-cluster.cmd
│   └── stop-seatunnel-cluster.sh
├── config
│   ├── hazelcast-client.yaml
│   ├── hazelcast-master.yaml
│   ├── hazelcast-worker.yaml
│   ├── hazelcast.yaml
│   ├── jvm_client_options
│   ├── jvm_master_options
│   ├── jvm_options
│   ├── jvm_worker_options
│   ├── log4j2.properties
│   ├── log4j2_client.properties
│   ├── plugin_config
│   ├── seatunnel-env.cmd
│   ├── seatunnel-env.sh
│   ├── seatunnel.yaml
│   ├── v2.batch.config.template
│   └── v2.streaming.conf.template
├── connectors
│   ├── connector-cdc-mysql-2.3.11.jar
│   ├── connector-cdc-oracle-2.3.11.jar
│   ├── connector-cdc-sqlserver-2.3.11.jar
│   ├── connector-console-2.3.11.jar
│   ├── connector-fake-2.3.11.jar
│   ├── connector-file-hadoop-2.3.11.jar
│   ├── connector-file-local-2.3.11.jar
│   ├── connector-file-s3-2.3.11.jar
│   ├── connector-file-sftp-2.3.11.jar
│   ├── connector-hive-2.3.11.jar
│   ├── connector-iceberg-2.3.11.jar
│   ├── connector-jdbc-2.3.11.jar
│   ├── connector-kafka-2.3.11.jar
│   ├── connector-paimon-2.3.11.jar
│   ├── plugin-mapping.properties
│   └── seatunnel-transforms-v2-2.3.11.jar
├── lib
│   ├── mssql-jdbc-9.5.0.jre8-preview.jar
│   ├── mysql-connector-java-8.0.28.jar
│   ├── seatunnel-hadoop-aws.jar
│   ├── seatunnel-hadoop3-3.1.4-uber.jar
│   ├── seatunnel-transforms-v2-2.3.11.jar
│   └── seatunnel-udf-1.0-SNAPSHOT.jar
├── licenses
│   ├── LICENSE-accessors-smart.txt
│   ├── LICENSE-animal-sniffer-annotations.txt
│   ├── LICENSE-asm.txt
│   ├── LICENSE-avro.txt
│   ├── LICENSE-checker-qual.txt
│   ├── LICENSE-codec-commons-codec.txt
│   ├── LICENSE-commons-beanutils.txt
│   ├── LICENSE-commons-cli.txt
│   ├── LICENSE-commons-collections.txt
│   ├── LICENSE-commons-compress.txt
│   ├── LICENSE-commons-configuration2.txt
│   ├── LICENSE-commons-io.txt
│   ├── LICENSE-commons-lang.txt
│   ├── LICENSE-commons-lang3.txt
│   ├── LICENSE-commons-math3.txt
│   ├── LICENSE-commons-net.txt
│   ├── LICENSE-connons-math.txt
│   ├── LICENSE-curator-client.txt
│   ├── LICENSE-curator-framework.txt
│   ├── LICENSE-curator-recipes.txt
│   ├── LICENSE-error-prone-annotations.txt
│   ├── LICENSE-findbugs-jsr305.txt
│   ├── LICENSE-gson.txt
│   ├── LICENSE-guava.txt
│   ├── LICENSE-hadoop-annotations.txt
│   ├── LICENSE-hadoop-auth.txt
│   ├── LICENSE-hadoop-client.txt
│   ├── LICENSE-hadoop-common.txt
│   ├── LICENSE-hadoop-hdfs-client.txt
│   ├── LICENSE-hadoop-mapreduce-client-common.txt
│   ├── LICENSE-hadoop-mapreduce-client-core.txt
│   ├── LICENSE-hadoop-yarn-api.txt
│   ├── LICENSE-hadoop-yarn-client.txt
│   ├── LICENSE-hadoop-yarn-common.txt
│   ├── LICENSE-htrace-core4.txt
│   ├── LICENSE-httpclient.txt
│   ├── LICENSE-j2objc-annotations.txt
│   ├── LICENSE-jackson-annotations.txt
│   ├── LICENSE-jackson-core-asl.txt
│   ├── LICENSE-jackson-core.txt
│   ├── LICENSE-jackson-databind.txt
│   ├── LICENSE-jackson-mapper-asl.txt
│   ├── LICENSE-javax-annootation-api.txt
│   ├── LICENSE-javax.servlet-api.txt
│   ├── LICENSE-jaxb-api.txt
│   ├── LICENSE-jcip-annotations.txt
│   ├── LICENSE-jersey-client.txt
│   ├── LICENSE-jersey-core.txt
│   ├── LICENSE-jersey-servlet.txt
│   ├── LICENSE-jetty-security.txt
│   ├── LICENSE-jetty-servlet.txt
│   ├── LICENSE-jetty-util.txt
│   ├── LICENSE-jetty-webapp.txt
│   ├── LICENSE-jetty-xml.txt
│   ├── LICENSE-jose-jwt.txt
│   ├── LICENSE-json-smart.txt
│   ├── LICENSE-jsr311-api.txt
│   ├── LICENSE-kerb-admin.txt
│   ├── LICENSE-kerb-client.txt
│   ├── LICENSE-kerb-common.txt
│   ├── LICENSE-kerb-core.txt
│   ├── LICENSE-kerb-crypto.txt
│   ├── LICENSE-kerb-identity.txt
│   ├── LICENSE-kerb-server.txt
│   ├── LICENSE-kerb-simplekdc.txt
│   ├── LICENSE-kerb-util.txt
│   ├── LICENSE-kerby-asn1.txt
│   ├── LICENSE-kerby-config.txt
│   ├── LICENSE-kerby-pkix.txt
│   ├── LICENSE-kerby-util.txt
│   ├── LICENSE-kerby-xdr.txt
│   ├── LICENSE-log4j-1.2-api.txt
│   ├── LICENSE-log4j-api.txt
│   ├── LICENSE-log4j-core.txt
│   ├── LICENSE-log4j-slf4j-impl.txt
│   ├── LICENSE-mapreduce-client-jobclient.txt
│   ├── LICENSE-orc.txt
│   ├── LICENSE-parquet-format.txt
│   ├── LICENSE-parquet-mr.txt
│   ├── LICENSE-protobuf-java.txt
│   ├── LICENSE-protobuf.txt
│   ├── LICENSE-protoc-jar.txt
│   ├── LICENSE-re2j.txt
│   ├── LICENSE-scala.txt
│   ├── LICENSE-sjf4j.txt
│   ├── LICENSE-snappy-java.txt
│   ├── LICENSE-spark.txt
│   ├── LICENSE-stax2-api.txt
│   ├── LICENSE-token-provider.txt
│   ├── LICENSE-woodstox-core.txt
│   ├── LICENSE-xz.txt
│   └── LICENSE-yetus.txt
├── logs
│   └── seatunnel-engine-master.log
├── mvnw
├── mvnw.cmd
├── plugins
│   └── README.md
└── starter
    ├── logging
    │   ├── jcl-over-slf4j-1.7.36.jar
    │   ├── log4j-api-2.17.1.jar
    │   ├── log4j-core-2.17.1.jar
    │   ├── log4j-slf4j-impl-2.17.1.jar
    │   └── slf4j-api-1.7.36.jar
    ├── seatunnel-flink-13-starter.jar
    ├── seatunnel-flink-15-starter.jar
    ├── seatunnel-spark-2-starter.jar
    ├── seatunnel-spark-3-starter.jar
    └── seatunnel-starter.jar

9 directories, 165 files

```

