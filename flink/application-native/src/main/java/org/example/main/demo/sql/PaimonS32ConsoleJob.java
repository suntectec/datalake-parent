package org.example.main.demo.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class PaimonS32ConsoleJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(PaimonS32ConsoleJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH (\n" +
                        "    'type'='paimon',\n" +
                        "    'warehouse'='s3://warehouse/paimon/',\n" +
                        "    's3.endpoint'='http://192.168.138.15:9000',\n" +
                        "    's3.access-key'='minioadmin',\n" +
                        "    's3.secret-key'='minioadmin',\n" +
                        "    's3.path.style.access'='true'\n" +
                        ");"
        );

        tEnv.sqlQuery("SELECT * FROM `paimon_catalog`.`my_db`.`T`")
                .execute()
                .print();
    }
}
