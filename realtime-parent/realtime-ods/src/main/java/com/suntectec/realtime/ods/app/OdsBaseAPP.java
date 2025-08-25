package com.suntectec.realtime.ods.app;

import com.suntectec.realtime.common.base.BaseAPP;
import com.suntectec.realtime.common.utils.PropertiesUtil;
import com.suntectec.realtime.ods.function.MyFlatMapFunction;
import com.suntectec.realtime.ods.schema.SqlserverDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerSource;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

/**
 * @author Jagger
 * @since 2025/8/18 16:26
 */
public class OdsBaseAPP extends BaseAPP {
    public static void main(String[] args) throws Exception {
        new OdsBaseAPP().start(8081, "test-group", "test-topic", args, OffsetsInitializer.earliest());
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {
        streamSource
                .flatMap(new MyFlatMapFunction())
                .print();

        // DataStreamSource<String> sqlServerDS = SqlserverUtil.createSqlServerCdcDataStream(env,
        //         parameter,
        //         SqlServerConstant.SQLSERVER_SOURCE_DB,
        //         SqlServerConstant.SQLSERVER_SOURCE_TB,
        //         StartupOptions.initial());
        //
        // sqlServerDS
        //         .print()
        //         .setParallelism(1);

        // 使用自定义 SqlServer Debezium Schema 和 时间日期 Converter
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
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // Flink处理程序被cancel后，会保留Checkpoint数据
        env.getCheckpointConfig().getExternalizedCheckpointRetention();

        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder<String>()
                        .hostname(parameter.get("sqlserver.host"))
                        .port(parameter.getInt("sqlserver.port"))
                        .username(parameter.get("sqlserver.username"))
                        .password(parameter.get("sqlserver.password"))
                        .databaseList("inventory")
                        .tableList("INV.orders")
                        .deserializer(new SqlserverDeserializationSchema())
                        .debeziumProperties(getDebeziumProperties())
                        .startupOptions(StartupOptions.initial())
                        .build();

        env.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(), "SqlServer Source")
                .print()
                .setParallelism(1);

    }

    public static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.put("converters", "sqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.type", "com.suntectec.realtime.ods.converter.SqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.database.type", "sqlserver");
        // 自定义格式，可选
        properties.put("sqlserverDebeziumConverter.format.date", "yyyy-MM-dd");
        properties.put("sqlserverDebeziumConverter.format.time", "HH:mm:ss");
        properties.put("sqlserverDebeziumConverter.format.datetime", "yyyy-MM-dd HH:mm:ss");
        return properties;
    }

}