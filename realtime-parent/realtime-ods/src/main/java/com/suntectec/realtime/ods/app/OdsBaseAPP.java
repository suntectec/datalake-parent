package com.suntectec.realtime.ods.app;

import com.alibaba.fastjson2.JSON;
import com.suntectec.realtime.common.base.BaseAPP;
import com.suntectec.realtime.common.bean.ods.SqlserverOrdersInputBean;
import com.suntectec.realtime.common.constant.SqlserverConstant;
import com.suntectec.realtime.common.constant.TopicConstant;
import com.suntectec.realtime.common.utils.FlinkSinkUtil;
import com.suntectec.realtime.ods.function.MyFlatMapFunction;
import com.suntectec.realtime.ods.function.OdsOrdersProcessFunction;
import com.suntectec.realtime.ods.source.SqlserverOdsSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将 flink sqlserver cdc debezium 数据转化成表数据写入 kafka
 *
 * @author Jagger
 * @since 2025/8/18 16:26
 */
@Slf4j
public class OdsBaseAPP extends BaseAPP {
    public static void main(String[] args) throws Exception {
        new OdsBaseAPP().start(8081, "test_group", "test_topic", args, OffsetsInitializer.earliest());
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

        SingleOutputStreamOperator<String> result = env.fromSource(SqlserverOdsSource.getSqlServerOdsSource(parameter, SqlserverConstant.SQLSERVER_SOURCE_DB, SqlserverConstant.SQLSERVER_SOURCE_TB), WatermarkStrategy.noWatermarks(), "SqlServer Source")
                .name("sqlserver-ods-orders")
                .map(v -> JSON.parseObject(v, SqlserverOrdersInputBean.class))
                .process(new OdsOrdersProcessFunction());

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        } else { // 写入kafka
            result.sinkTo(FlinkSinkUtil.getKafkaSink(parameter, TopicConstant.TOPIC_ODS_ORDERS)).name("sink_orders_topic");
        }

    }

}
