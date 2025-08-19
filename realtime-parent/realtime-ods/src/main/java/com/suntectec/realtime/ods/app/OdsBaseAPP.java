package com.suntectec.realtime.ods.app;

import com.suntectec.realtime.common.base.BaseAPP;
import com.suntectec.realtime.common.utils.SqlServerUtil;
import com.suntectec.realtime.ods.function.MyFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        DataStreamSource<String> sqlServerDS = SqlServerUtil.createSqlServerCdcDataStream(env,
                parameter,
                "inventory",
                "INV.orders",
                StartupOptions.initial());

        sqlServerDS
                .print();

    }

}