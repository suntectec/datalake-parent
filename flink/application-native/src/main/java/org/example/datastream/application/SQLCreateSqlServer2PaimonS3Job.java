package org.example.datastream.application;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class SQLCreateSqlServer2PaimonS3Job {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(SQLCreateSqlServer2PaimonS3Job.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE Orders (\n" +
                        "    id BIGINT,\n" +
                        "    order_id VARCHAR(36),\n" +
                        "    supplier_id INT,\n" +
                        "    item_id INT,\n" +
                        "    status VARCHAR(20),\n" +
                        "    qty INT,\n" +
                        "    net_price INT,\n" +
                        "    issued_at TIMESTAMP,\n" +
                        "    completed_at TIMESTAMP,\n" +
                        "    spec VARCHAR(1024),\n" +
                        "    created_at TIMESTAMP,\n" +
                        "    updated_at TIMESTAMP,\n" +
                        "    PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "    'connector' = 'sqlserver-cdc',\n" +
                        "    'hostname' = '192.168.138.15',\n" +
                        "    'port' = '1433',\n" +
                        "    'username' = 'sa',\n" +
                        "    'password' = 'Abcd1234',\n" +
                        "    'database-name' = 'inventory',\n" +
                        "    'table-name' = 'INV.orders'\n" +
                        ");"
        );

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

        TableResult tableResult = tEnv.executeSql(
                "INSERT INTO paimon_catalog.inventory.orders SELECT * FROM Orders"
        );
        if (tableResult.getJobClient().isPresent())
            System.out.println(tableResult.getJobClient().get().getJobStatus());
    }
}
