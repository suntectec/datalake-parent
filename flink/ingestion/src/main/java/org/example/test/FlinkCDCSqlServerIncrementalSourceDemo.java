package org.example.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCSqlServerIncrementalSourceDemo {
    public static void main(String[] args) throws Exception {
        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder()
                            .hostname("dev-ds-trm01.tailb6e5ab.ts.net")
                        .port(1433)
                        .databaseList("inventory")
                        .tableList("INV.orders")
                        .username("sa")
                        .password("StrongPassword123!")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(
                        sqlServerSource,
                        WatermarkStrategy.noWatermarks(),
                        "SqlServerIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print SqlServer Snapshot + Change Stream");

    }
}
