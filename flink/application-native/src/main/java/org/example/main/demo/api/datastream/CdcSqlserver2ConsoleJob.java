package org.example.main.demo.api.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

;

/**
 * @author Jagger
 * @since 2025/8/6 11:40
 */
public class CdcSqlserver2ConsoleJob {
    private static final Logger logger = LoggerFactory.getLogger(CdcSqlserver2ConsoleJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String warehouse, String s3_endpoint, String s3_access_key, String s3_secret_key,
                           String kafka_brokers) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // CDC Ingestion source from SQL Server
        SqlServerIncrementalSource<String> sqlServerIncrementalSource = new SqlServerSourceBuilder<String>()
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

        dataStreamSource.print();

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = PropertiesUtil.getProperty("sqlserver.host");
        String sqlserver_port = PropertiesUtil.getProperty("sqlserver.port");
        String sqlserver_username = PropertiesUtil.getProperty("sqlserver.username");
        String sqlserver_password = PropertiesUtil.getProperty("sqlserver.password");

        String warehouse = PropertiesUtil.getProperty("warehouse");
        String s3_endpoint = PropertiesUtil.getProperty("s3.endpoint");
        String s3_access_key = PropertiesUtil.getProperty("s3.access-key");
        String s3_secret_key = PropertiesUtil.getProperty("s3.secret-key");

        String kafka_brokers = PropertiesUtil.getProperty("kafka.brokers");

        CdcSqlserver2ConsoleJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                warehouse, s3_endpoint, s3_access_key, s3_secret_key, kafka_brokers);
    }
}
