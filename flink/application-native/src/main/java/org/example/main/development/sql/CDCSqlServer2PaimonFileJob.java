package org.example.main.development.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.util.MyParameter;
import org.example.util.MySqlExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/7/30 18:00
 */
public class CDCSqlServer2PaimonFileJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(CDCSqlServer2PaimonFileJob.class);

    public static void run(String sqlServerHost, String sqlServerPort, String sqlServerUsername, String sqlServerPassword) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceSQL =
                "CREATE TABLE Orders (\n" +
                        "                                  id BIGINT,\n" +
                        "                                  username BIGINT,\n" +
                        "                                  product VARCHAR(64),\n" +
                        "                                  amount INT\n" +
                        ") WITH (\n" +
                        "      'connector' = 'sqlserver-cdc',\n" +
                        "      'hostname' = '"+sqlServerHost+"',\n" +
                        "      'port' = '"+sqlServerPort+"',\n" +
                        "      'username' = '"+sqlServerUsername+"',\n" +
                        "      'password' = '"+sqlServerPassword+"',\n" +
                        "      'database-name' = 'TestDB',\n" +
                        "      'scan.startup.mode' = 'latest-offset',\n" +
                        "      'table-name' = 'dbo.orders'\n" +
                        "      );";

        String sinkSQL =
                "CREATE TABLE PaimonOrders (\n" +
                        "                                  id BIGINT,\n" +
                        "                                  username BIGINT,\n" +
                        "                                  product VARCHAR(64),\n" +
                        "                                  amount INT\n" +
                        ") WITH (\n" +
                        "      'connector' = 'paimon',\n" +
                        "      'path' = 'file:///tmp/paimon/warehouse',\n" +
                        "      'auto-create' = 'true'\n" +
                        "      );";

        // MySqlExecutor.executeSQL(tEnv, sourceSQL);
        // MySqlExecutor.executeSQL(tEnv, sinkSQL);
        // MySqlExecutor.executeSQL(tEnv, "INSERT INTO PaimonOrders SELECT * FROM Orders");
        MySqlExecutor.executeBatch(tEnv, new String[]{sourceSQL, sinkSQL, "INSERT INTO PaimonOrders SELECT * FROM Orders"});
    }

    public static void main(String[] args) throws Exception {
        String sqlServerHost = MyParameter.getParameter("dev", "sqlserver.host");
        String sqlServerPort = MyParameter.getParameter("dev", "sqlserver.port");
        String sqlServerUsername = MyParameter.getParameter("dev", "sqlserver.username");
        String sqlServerPassword = MyParameter.getParameter("dev", "sqlserver.password");

        String mySqlServerHost = MyParameter.getParameter("dev", "my.sqlserver.host");
        String mySqlServerPort = MyParameter.getParameter("dev", "my.sqlserver.port");
        String mySqlServerUsername = MyParameter.getParameter("dev", "my.sqlserver.username");
        String mySqlServerPassword = MyParameter.getParameter("dev", "my.sqlserver.password");

        CDCSqlServer2PaimonFileJob.run(mySqlServerHost, mySqlServerPort, mySqlServerUsername, mySqlServerPassword);

    }
}
