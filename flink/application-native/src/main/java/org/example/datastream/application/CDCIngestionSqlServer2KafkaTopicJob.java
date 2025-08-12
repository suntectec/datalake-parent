package org.example.datastream.application;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.MyParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/6 11:40
 */
public class CDCIngestionSqlServer2KafkaTopicJob {
    private static final Logger logger = LoggerFactory.getLogger(CDCIngestionSqlServer2KafkaTopicJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String warehouse, String s3_endpoint, String s3_access_key, String s3_secret_key,
                           String kafka_brokers) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // CDC Ingestion source from SQL Server
        SqlServerSourceBuilder.SqlServerIncrementalSource<String> sqlServerIncrementalSource =
                new SqlServerSourceBuilder<String>()
                        .hostname(sqlserver_host)
                        .port(Integer.parseInt(sqlserver_port))
                        .databaseList("inventory")
                        .tableList("INV.orders")
                        .username(sqlserver_username)
                        .password(sqlserver_password)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(sqlServerIncrementalSource, WatermarkStrategy.noWatermarks(), "SqlServer Source");

        // CDC Ingestion sink to Kafka Topic
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafka_brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("SqlServer.Orders")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        dataStreamSource.print();
        dataStreamSource.sinkTo(kafkaSink);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = MyParameter.getParameter("dev", "sqlserver.host");
        String sqlserver_port = MyParameter.getParameter("dev", "sqlserver.port");
        String sqlserver_username = MyParameter.getParameter("dev", "sqlserver.username");
        String sqlserver_password = MyParameter.getParameter("dev", "sqlserver.password");

        String warehouse = MyParameter.getParameter("dev", "warehouse");
        String s3_endpoint = MyParameter.getParameter("dev", "s3.endpoint");
        String s3_access_key = MyParameter.getParameter("dev", "s3.access-key");
        String s3_secret_key = MyParameter.getParameter("dev", "s3.secret-key");

        String kafka_brokers = MyParameter.getParameter("dev", "kafka.brokers");

        CDCIngestionSqlServer2KafkaTopicJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                warehouse, s3_endpoint, s3_access_key, s3_secret_key, kafka_brokers);
    }
}
