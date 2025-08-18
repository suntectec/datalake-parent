package org.example.realtime.demo.api.table;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/13 10:10
 */
public class Datagen2BlackholeJob {
    private static final Logger logger= LoggerFactory.getLogger(Datagen2BlackholeJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

        tEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector'='blackhole') LIKE SourceTable (EXCLUDING OPTIONS)");

        tEnv.sqlQuery("SELECT * FROM SourceTable")
            .execute()
            .print();

        // tEnv.sqlQuery("SELECT * FROM SourceTable")
        //         .executeInsert("SinkTable");

        tEnv.from("SourceTable").insertInto("SinkTable").execute();
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = PropertiesUtil.getProperty("sqlserver.host");
        String sqlserver_port = PropertiesUtil.getProperty("sqlserver.port");
        String sqlserver_username = PropertiesUtil.getProperty("sqlserver.username");
        String sqlserver_password = PropertiesUtil.getProperty("sqlserver.password");

        String s3_endpoint = PropertiesUtil.getProperty("s3.endpoint");
        String s3_access_key = PropertiesUtil.getProperty("s3.access-key");
        String s3_secret_key = PropertiesUtil.getProperty("s3.secret-key");

        Datagen2BlackholeJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                s3_endpoint, s3_access_key, s3_secret_key);
    }
}