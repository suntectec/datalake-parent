package org.example.main.demo.api.datastream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/14 14:04
 */
public class Kafka2UpsertKafkaJob {
    private static Logger logger = LoggerFactory.getLogger(Kafka2UpsertKafkaJob.class);

    public static void run(String kafka_brokers) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // TableConfig tableConfig = tEnv.getConfig();
        // tableConfig.set("table.exec.mini-batch.enabled", "true");
        // tableConfig.set("table.exec.mini-batch.allow-latency", "5s");
        // tableConfig.set("table.exec.mini-batch.size", "5000");
        // TABLE CHANGELOG TABLEAU
        // tableConfig.set("sql-client.execution.result-mode", "TABLE");

        // Source
        /*{"user_id":1, "page_id":1, "viewtime":"2025-08-14 12:00:00", "user_region":"US"}*/
        tEnv.executeSql("CREATE TABLE pageviews (\n" +
                "  user_id BIGINT,\n" +
                "  page_id BIGINT,\n" +
                "  viewtime TIMESTAMP(3),\n" +
                "  user_region STRING,\n" +
                "  WATERMARK FOR viewtime AS viewtime - INTERVAL '2' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'pageviews',\n" +
                "  'properties.bootstrap.servers' = '192.168.138.15:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ");");

        // Sink
        // As a source, the upsert-kafka connector produces a changelog stream, where each data record represents an update or delete event.
        // 若使用upsert-kafka作为结果表，连接器可以消费上游计算逻辑产生的Changelog流，并将INSERT或UPDATE_AFTER数据写入的Kafka，将DELETE数据以value为空的消息写入，表示对应key的消息被删除。Flink将根据主键列的值对数据进行分区，从而保证主键上的消息有序，同一主键上的更新或删除消息将落在同一分区中。
        tEnv.executeSql("CREATE TABLE pageviews_per_region (\n" +
                "  user_region STRING,\n" +
                "  pv BIGINT,\n" +
                "  uv BIGINT,\n" +
                "  PRIMARY KEY (user_region) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'pageviews_per_region',\n" +
                "  'properties.bootstrap.servers' = '192.168.138.15:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json',\n" +
                "  'value.fields-include' = 'EXCEPT_KEY',\n" +
                "  'key.json.ignore-parse-errors' = 'true',\n" +
                "  'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        // -- 计算 pv、uv 并插入到 upsert-kafka sink
        tEnv.executeSql("INSERT INTO pageviews_per_region\n" +
                "                SELECT\n" +
                "        user_region,\n" +
                "                COUNT(*),\n" +
                "        COUNT(DISTINCT user_id)\n" +
                "        FROM pageviews\n" +
                "        GROUP BY user_region;");

        tEnv.executeSql("SELECT * FROM pageviews_per_region").print();
    }

    public static void main(String[] args) throws Exception {
        String kafka_brokers = PropertiesUtil.getProperty("kafka.brokers");

        Kafka2UpsertKafkaJob.run(kafka_brokers);
    }
}
