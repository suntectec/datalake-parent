package org.example.main.development.api.datastream;

import com.alibaba.fastjson2.JSON;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.example.util.MyParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/11 15:56
 */
public class KafkaRichCdcRecord2ConsoleJob {
    public static final Logger loggger = LoggerFactory.getLogger(KafkaRichCdcRecord2ConsoleJob.class);

    public static void run(String kafka_brokers) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafka_brokers)
                .setTopics("PartialUpdate")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<RichCdcRecord> streamOperator = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(JSON::parseObject)
                .map(jsonObj -> {
                    val domain = jsonObj.getString("domain");
                    val registrar = jsonObj.getString("registrar");
                    val registrant_org = jsonObj.getString("registrant_org");
                    val registrar_country = jsonObj.getString("registrar_country");
                    val registrant_province = jsonObj.getString("registrant_province");
                    val merge_num = "1";

                    // return Row.ofKind(RowKind.INSERT,
                    //         domain,
                    //         registrar,
                    //         registrant_org,
                    //         registrar_country,
                    //         registrant_province,
                    //         merge_num);
                    return RichCdcRecord.builder(RowKind.INSERT)
                            .field("domain", DataTypes.STRING(), domain)
                            .field("registrar", DataTypes.STRING(), registrar)
                            .field("registrant_org", DataTypes.STRING(), registrant_org)
                            .field("registrar_country", DataTypes.STRING(), registrar_country)
                            .field("registrant_province", DataTypes.STRING(), registrant_province)
                            .field("merge_num", DataTypes.INT(), merge_num)
                            .build();
                })
                .setParallelism(1);

        streamOperator.print();

        env.execute();
    }

    public static void main(String[] args) throws Exception {

        String kafka_brokers = MyParameter.getParameter("dev", "kafka.brokers");

        KafkaRichCdcRecord2ConsoleJob.run(kafka_brokers);
    }
}
