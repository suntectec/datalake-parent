package org.example.realtime.demo.api.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception in thread "main" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
 * - issue temporarily resolved by add hadoop-3.4.0-win10-x64/bin/hadoop.dll to Windows/System32
 *
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class CdcMysql2ConsoleJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(CdcMysql2ConsoleJob.class);

    public static void run(String mysql_host,String mysql_port,String mysql_username,String mysql_password) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        env.enableCheckpointing(3000);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysql_host)
                .port(Integer.parseInt(mysql_port))
                .databaseList("inventory") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("inventory.products") // set captured table
                .username(mysql_username)
                .password(mysql_password)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.earliest()) // Start from earliest offset
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }

    public static void main(String[] args) throws Exception {
        String mysql_host = PropertiesUtil.getProperty("mysql.hostname");
        String mysql_port = PropertiesUtil.getProperty("mysql.port");
        String mysql_username = PropertiesUtil.getProperty("mysql.username");
        String mysql_password = PropertiesUtil.getProperty("mysql.password");

        CdcMysql2ConsoleJob.run(mysql_host, mysql_port, mysql_username, mysql_password);
    }
}
