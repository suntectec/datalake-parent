package org.example.main.dev.api.datastream;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.CustomerParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception in thread "main" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
 * - issue temporarily resolved by add hadoop-3.4.0-win10-x64/bin/hadoop.dll to Windows/System32
 *
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class CdcSqlserverMapFunction2ConsoleJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(CdcSqlserverMapFunction2ConsoleJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        env.enableCheckpointing(3000);

        SqlServerIncrementalSource<String> sqlServerSource =
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

        // set the source parallelism to 2
        DataStreamSource<String> sourceDS = env.fromSource(
                sqlServerSource,
                WatermarkStrategy.noWatermarks(),
                "SqlServerIncrementalSource");
        sourceDS
                .setParallelism(2)
                .map(JSON::parseObject)
                .map(jsonObject -> {
                    JSONObject beforeJson = new JSONObject();
                    if (jsonObject.getJSONObject("before") != null) {
                        beforeJson = jsonObject.getJSONObject("before");
                    }
                    JSONObject afterJson = new JSONObject();
                    if (jsonObject.getJSONObject("after") != null) {
                        afterJson = jsonObject.getJSONObject("after");
                    }
                    JSONObject sourceJson = new JSONObject();
                    if (jsonObject.getJSONObject("source") != null) {
                        sourceJson = jsonObject.getJSONObject("source");
                    }
                    JSONObject transactionJson = new JSONObject();
                    if (transactionJson.getJSONObject("transaction") != null) {
                        transactionJson = jsonObject.getJSONObject("transaction");
                    }
                    String op = jsonObject.getString("op");
                    String ts_ms = jsonObject.getString("ts_ms");

                    // Log the operation type and timestamp
                    logger.info("Operation: {}, Timestamp: {}", op, ts_ms);

                    JSONObject result = new JSONObject();
                    result.put("before", beforeJson);
                    result.put("after", afterJson);
                    result.put("source", sourceJson);
                    result.put("op", op);
                    result.put("ts_ms", ts_ms);
                    result.put("transaction", transactionJson);

                    return result.toJSONString();
                })
                .print()
                .setParallelism(1);

        env.execute("Print SqlServer Snapshot + Change Stream");
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = CustomerParameter.getParameter("dev", "sqlserver.host");
        String sqlserver_port = CustomerParameter.getParameter("dev", "sqlserver.port");
        String sqlserver_username = CustomerParameter.getParameter("dev", "sqlserver.username");
        String sqlserver_password = CustomerParameter.getParameter("dev", "sqlserver.password");

        String s3_endpoint = CustomerParameter.getParameter("dev", "s3.endpoint");
        String s3_access_key = CustomerParameter.getParameter("dev", "s3.access-key");
        String s3_secret_key = CustomerParameter.getParameter("dev", "s3.secret-key");

        CdcSqlserverMapFunction2ConsoleJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                s3_endpoint, s3_access_key, s3_secret_key);
    }
}
