package org.example.main.dev.api.table;

import org.apache.flink.connector.jdbc.core.table.JdbcConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.util.CustomerParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/13 10:10
 */
public class JdbcSqlserver2BlackholeJob {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSqlserver2BlackholeJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("jdbc")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("order_id", DataTypes.STRING())
                        .column("supplier_id", DataTypes.INT())
                        .column("item_id", DataTypes.INT())
                        .column("status", DataTypes.STRING())
                        .column("qty", DataTypes.INT())
                        .column("net_price", DataTypes.INT())
                        .column("issued_at", DataTypes.TIMESTAMP())
                        .column("completed_at", DataTypes.TIMESTAMP())
                        .column("spec", DataTypes.STRING())
                        .column("created_at", DataTypes.TIMESTAMP())
                        .column("updated_at", DataTypes.TIMESTAMP())
                        .primaryKey("id")
                        .build())
                .option(JdbcConnectorOptions.URL, "jdbc:sqlserver://" + sqlserver_host + ":" + sqlserver_port + ";database=inventory;Encrypt=false;trustServerCertificate=true")
                .option(JdbcConnectorOptions.USERNAME, sqlserver_username)
                .option(JdbcConnectorOptions.PASSWORD, sqlserver_password)
                .option(JdbcConnectorOptions.TABLE_NAME, "inventory.INV.orders")
                .option(JdbcConnectorOptions.DRIVER, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .build());

        tEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector'='blackhole') LIKE SourceTable (EXCLUDING OPTIONS)");

        tEnv.sqlQuery("SELECT * FROM SourceTable")
                .execute()
                .print();

        tEnv.sqlQuery("SELECT * FROM SourceTable")
                .executeInsert("SinkTable");

        // tEnv.from("orders").insertInto("SinkTable").execute();
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = CustomerParameter.getParameter("dev", "sqlserver.host");
        String sqlserver_port = CustomerParameter.getParameter("dev", "sqlserver.port");
        String sqlserver_username = CustomerParameter.getParameter("dev", "sqlserver.username");
        String sqlserver_password = CustomerParameter.getParameter("dev", "sqlserver.password");

        String s3_endpoint = CustomerParameter.getParameter("dev", "s3.endpoint");
        String s3_access_key = CustomerParameter.getParameter("dev", "s3.access-key");
        String s3_secret_key = CustomerParameter.getParameter("dev", "s3.secret-key");

        JdbcSqlserver2BlackholeJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                s3_endpoint, s3_access_key, s3_secret_key);
    }
}