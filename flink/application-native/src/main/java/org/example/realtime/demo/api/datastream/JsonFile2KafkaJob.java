package org.example.realtime.demo.api.datastream;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

/**
 * @author Jagger
 * @since 2025/8/8 15:55
 */
public class JsonFile2KafkaJob {
    private static final Logger logger = LoggerFactory.getLogger(JsonFile2KafkaJob.class);
    public static void run(String kafka_brokers) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), Path.fromLocalFile(new File("flink/application-native/src/main/resources/datasource/json")))
                .monitorContinuously(Duration.ofSeconds(1L))
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        JsonSerializationSchema<JSONObject> jsonFormat = new JsonSerializationSchema<>();
        KafkaSink<JSONObject> kafkaSink = KafkaSink.<JSONObject>builder()
                .setBootstrapServers(kafka_brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("PartialUpdate")
                        .setValueSerializationSchema(jsonFormat)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        dataStreamSource
                .setParallelism(2)
                .map(JSON::parseObject)
                .sinkTo(kafkaSink)
                .setParallelism(1);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        String kafka_brokers = PropertiesUtil.getProperty("kafka.brokers");

        run(kafka_brokers);
    }
}
