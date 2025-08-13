package org.example.main.dev.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.util.CustomerParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/13 10:10
 */
public class CdcSqlserver2MysqlJob {
    private static final Logger logger= LoggerFactory.getLogger(CdcSqlserver2MysqlJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TEMPORARY TABLE SourceTable (\n" +
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
                ");");

        tEnv.executeSql("CREATE TABLE SinkTable (\n" +
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
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.138.15:3306/inventory',\n" +
                "    'table-name' = 'sink_orders',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ");");

        tEnv.executeSql("INSERT INTO SinkTable SELECT * FROM SourceTable;");

        tEnv.executeSql("SELECT * FROM SinkTable;").print();
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = CustomerParameter.getParameter("dev", "sqlserver.host");
        String sqlserver_port = CustomerParameter.getParameter("dev", "sqlserver.port");
        String sqlserver_username = CustomerParameter.getParameter("dev", "sqlserver.username");
        String sqlserver_password = CustomerParameter.getParameter("dev", "sqlserver.password");

        String s3_endpoint = CustomerParameter.getParameter("dev", "s3.endpoint");
        String s3_access_key = CustomerParameter.getParameter("dev", "s3.access-key");
        String s3_secret_key = CustomerParameter.getParameter("dev", "s3.secret-key");

        CdcSqlserver2MysqlJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                s3_endpoint, s3_access_key, s3_secret_key);
    }
}