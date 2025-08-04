package org.example.datastream.application;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.MyParameter;

/**
 * @author Jagger
 * @since 2025/8/6 14:12
 */
public class KafkaTopic2ConsoleJob {
    public static void run(String kafka_brokers) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafka_brokers)
                .setTopics("SqlServer.Orders")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .print();

        env.execute();
    }

    public static void main(String[] args) throws Exception {

        String kafka_brokers = MyParameter.getParameter("dev", "kafka.brokers");

        KafkaTopic2ConsoleJob.run(kafka_brokers);
    }
}
