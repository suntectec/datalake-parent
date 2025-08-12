package org.example.main.development.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/8/8 15:38
 */
public class Kafka2ConsoleJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "create table KafkaPartialUpdateSource (\n" +
                        "                  domain STRING,\n" +
                        "                  registrar STRING,\n" +
                        "                  registrant_org STRING,\n" +
                        "                  registrar_country STRING,\n" +
                        "                  registrant_province STRING\n" +
                        "                  )\n" +
                        "                  with(\n" +
                        "                  'connector' = 'kafka',\n" +
                        "                  'topic' = 'PartialUpdate',\n" +
                        "                  'properties.bootstrap.servers' = '192.168.138.15:9092',\n" +
                        "                  'properties.group.id' = 'FlinkSQLReadKafka2Paimon',\n" +
                        "                  'scan.startup.mode' = 'earliest-offset',\n" +
                        "                  'format'='json'\n" +
                        "                  )");

        // TableResult tableResult = tEnv.executeSql("SELECT * FROM KafkaPartialUpdateSource");
        // tableResult.print();

        TableResult tableResult = tEnv.sqlQuery("SELECT * FROM KafkaPartialUpdateSource").execute();
        tableResult.print();
    }
}
