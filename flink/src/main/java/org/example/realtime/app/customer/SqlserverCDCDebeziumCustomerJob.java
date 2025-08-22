package org.example.realtime.app.customer;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerSource;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.utils.PropertiesUtil;

import java.util.Properties;

/**
 * @author Jagger
 * @since 2025/8/19 13:41
 */
public class SqlserverCDCDebeziumCustomerJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度
        env.setParallelism(1);
        // 设置时间语义为ProcessingTime
        env.getConfig().setAutoWatermarkInterval(0);
        // 每隔60s启动一个检查点
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        // checkpoint最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许一个checkpoint
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // Flink处理程序被cancel后，会保留Checkpoint数据
        //   env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        SourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname(PropertiesUtil.getProperty("sqlserver.host"))
                .port(Integer.parseInt(PropertiesUtil.getProperty("sqlserver.port")))
                .username(PropertiesUtil.getProperty("sqlserver.username"))
                .password(PropertiesUtil.getProperty("sqlserver.password"))
                .database("inventory")
                .tableList("INV.orders")
                .startupOptions(StartupOptions.initial())
                // {"before":null,"after":{"id":2,"order_id":"6eaa804c-5d1d-4b2f-ac92-021783a10d87","supplier_id":3016,"item_id":47,"status":"shipped","qty":600,"net_price":1310,"issued_at":1753243341153,"completed_at":1753243341153,"spec":null,"created_at":1753243341153,"updated_at":1753271606100},"source":{"version":"1.9.8.Final","connector":"sqlserver","name":"sqlserver_transaction_log_source","ts_ms":1755584849460,"snapshot":"last","db":"inventory","sequence":null,"schema":"INV","table":"orders","change_lsn":null,"commit_lsn":"0000002e:00002a40:0003","event_serial_no":null},"op":"r","ts_ms":1755584849448,"transaction":null}
                // .deserializer(new JsonDebeziumDeserializationSchema())
                // {"op":"READ","after":{"id":2,"order_id":"6eaa804c-5d1d-4b2f-ac92-021783a10d87","supplier_id":3016,"item_id":47,"status":"shipped","qty":600,"net_price":1310,"issued_at":"2025-07-23 04:02:21","completed_at":"2025-07-23 04:02:21","created_at":"2025-07-23 04:02:21","updated_at":"2025-07-23 11:53:26"},"db":"INV","tableName":"orders"}
                .debeziumProperties(getDebeziumProperties())
                .deserializer(new SqlserverDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source");
        dataStreamSource.print().setParallelism(1);
        env.execute("sqlserver-cdc-test");

    }


    public static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.put("converters", "sqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.type", "org.example.realtime.app.customer.SqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.database.type", "sqlserver");
        // 自定义格式，可选
        properties.put("sqlserverDebeziumConverter.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.put("sqlserverDebeziumConverter.format.date", "yyyy-MM-dd");
        properties.put("sqlserverDebeziumConverter.format.time", "HH:mm:ss");
        return properties;
}
}
