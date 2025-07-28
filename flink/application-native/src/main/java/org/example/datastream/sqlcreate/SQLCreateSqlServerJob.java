package org.example.datastream.sqlcreate;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/7/28 17:32
 */
public class SQLCreateSqlServerJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sqlStatement = "CREATE TABLE sqlserver_source (\n" +
                "                                  id BIGINT,\n" +
                "                                  order_id VARCHAR(36),\n" +
                "                                  supplier_id INT,\n" +
                "                                  item_id INT,\n" +
                "                                  status VARCHAR(20),\n" +
                "                                  qty INT,\n" +
                "                                  net_price INT,\n" +
                "                                  issued_at TIMESTAMP,\n" +
                "                                  completed_at TIMESTAMP,\n" +
                "                                  spec VARCHAR(1024),\n" +
                "                                  created_at TIMESTAMP,\n" +
                "                                  updated_at TIMESTAMP,\n" +
                "                                  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "      'connector' = 'sqlserver-cdc',\n" +
                "      'hostname' = '192.168.138.15',\n" +
                "      'port' = '1433',\n" +
                "      'username' = 'SA',\n" +
                "      'password' = 'Abcd1234',\n" +
                "      'database-name' = 'inventory',\n" +
                "      'table-name' = 'INV.orders'\n" +
                "      );";
        tableEnv.executeSql(sqlStatement);
        tableEnv.executeSql("SELECT * FROM sqlserver_source")
                .print();
    }
}
