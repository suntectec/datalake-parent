package com.suntectec.realtime.ods.app;

import com.suntectec.realtime.common.util.FlinkSourceUtils;
import com.suntectec.realtime.common.util.PropertiesUtils;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jagger
 * @since 2025/8/18 16:26
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String kafka_brokers = PropertiesUtils.getProperty("kafka.brokers");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> kafkaDSS = FlinkSourceUtils.createKafkaDataStream(env, kafka_brokers, "test-group", "pageviews", OffsetsInitializer.earliest());

        kafkaDSS.print();

        env.execute();
    }
}
