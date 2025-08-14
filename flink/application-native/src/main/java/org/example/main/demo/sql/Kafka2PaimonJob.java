package org.example.main.demo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/8/8 15:38
 */
public class Kafka2PaimonJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 第一步：读取kafka数据源
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

        // TableResult tableResult = tEnv.sqlQuery("SELECT * FROM KafkaPartialUpdateSource").execute();
        // tableResult.print();

        // 第二步：创建 paimon 的 catalog
        tEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH (\n" +
                        "    'type'='paimon',\n" +
                        "    'warehouse'='s3://lakehouse/paimon/',\n" +
                        "    's3.endpoint'='http://192.168.138.15:9000',\n" +
                        "    's3.access-key'='minioadmin',\n" +
                        "    's3.secret-key'='minioadmin',\n" +
                        "    's3.path.style.access'='true'\n" +
                        ");\n"
        );

        // 第三步，创建库
        tEnv.executeSql(
                "CREATE DATABASE IF NOT EXISTS `paimon_catalog`.`paimon_db`"
        );

        // 第四步，创建表
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS paimon_catalog.paimon_db.partial_update (\n" +
                        "  domain string NOT NULL,\n" +
                        "  registrar string,\n" +
                        "  registrant_org string,\n" +
                        "  registrar_country string,\n" +
                        "  registrant_province string,\n" +
                        "  merge_num INT,\n" +
                        "  PRIMARY KEY (domain) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'merge-engine' = 'partial-update',\n" +
                        "  'fields.registrar.sequence-group' = 'registrar',\n" +
                        "  'fields.registrant_org.sequence-group' = 'registrant_org',\n" +
                        "  'fields.registrar_country.sequence-group' = 'registrar_country',\n" +
                        "  'fields.registrant_province.sequence-group' = 'registrant_province,merge_num',\n" +
                        "  'fields.merge_num.aggregate-function' = 'sum'\n" +
                        ")"
        );

        // 第五步：执行数据写入
        TableResult tableResult = tEnv.executeSql(
                "INSERT INTO paimon_catalog.paimon_db.partial_update\n" +
                        "              select\n" +
                        "              domain,\n" +
                        "              registrar,\n" +
                        "              registrant_org,\n" +
                        "              registrar_country,\n" +
                        "              registrant_province,\n" +
                        "              1\n" +
                        "              from\n" +
                        "              KafkaPartialUpdateSource"
        );
        System.out.println(tableResult.getJobClient().get().getJobStatus());
    }
}
