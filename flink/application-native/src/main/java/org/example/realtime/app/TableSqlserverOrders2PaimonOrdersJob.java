package org.example.realtime.app;

import org.apache.flink.connector.jdbc.core.table.JdbcConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink 1.20.2 Table API Not Support Paimon Sink:
 * https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/overview/
 *
 * We can use SQL API Paimon Sink or DataStream API Paimon Sink as a substitute.
 *
 * @author Jagger
 * @since 2025/8/13 10:10
 */
public class TableSqlserverOrders2PaimonOrdersJob {
    private static final Logger logger = LoggerFactory.getLogger(TableSqlserverOrders2PaimonOrdersJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // SqlServer Source
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

        // Paimon Sink by Table API or Datatream

        // Insert

    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = PropertiesUtil.getProperty("sqlserver.host");
        String sqlserver_port = PropertiesUtil.getProperty("sqlserver.port");
        String sqlserver_username = PropertiesUtil.getProperty("sqlserver.username");
        String sqlserver_password = PropertiesUtil.getProperty("sqlserver.password");

        String s3_endpoint = PropertiesUtil.getProperty("s3.endpoint");
        String s3_access_key = PropertiesUtil.getProperty("s3.access-key");
        String s3_secret_key = PropertiesUtil.getProperty("s3.secret-key");

        TableSqlserverOrders2PaimonOrdersJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                s3_endpoint, s3_access_key, s3_secret_key);
    }
}