package org.example.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqlServerToPaimonWholeDatabaseSync {

    public static void main(String[] args) throws Exception {
        // 1. 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000); // 30秒checkpoint一次
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 2. SQL Server连接信息
        String sqlServerHost = "dev-ds-trm01.tailb6e5ab.ts.net";
        int sqlServerPort = 1433;
        String sqlServerDatabase = "poc_db";
        String sqlServerSchema = "lab"; // 默认schema
        String sqlServerUser = "flink";
        String sqlServerPassword = "flink";

        // 3. Paimon仓库路径
        String paimonWarehousePath = "file:///tmp/paimon/warehouse"; // 或 hdfs://路径

        // 4. 获取SQL Server中所有表名
        List<String> tableNames = getAllTablesFromSqlServer(
                sqlServerHost, sqlServerPort, sqlServerDatabase,
                sqlServerUser, sqlServerPassword);

        // 5. 为每个表创建同步任务
        for (String tableName : tableNames) {
            syncSingleTable(tEnv, sqlServerHost, sqlServerPort, sqlServerDatabase,
                    sqlServerSchema, tableName, sqlServerUser, sqlServerPassword,
                    paimonWarehousePath);
        }

        // 6. 执行作业
        env.execute("SQLServer Whole Database Sync to Paimon");
    }

    // 获取SQL Server中所有表名
    private static List<String> getAllTablesFromSqlServer(
            String host, int port, String database,
            String user, String password) throws SQLException {

        List<String> tables = new ArrayList<>();
        String url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, database);

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")) {

            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    // 同步单个表
    private static void syncSingleTable(
            StreamTableEnvironment tEnv,
            String sqlServerHost, int sqlServerPort, String sqlServerDatabase,
            String sqlServerSchema, String tableName,
            String sqlServerUser, String sqlServerPassword,
            String paimonWarehousePath) {

        // 1. 创建SQL Server CDC源表
        String sourceDDL = String.format(
                "CREATE TABLE sqlserver_source_%s ("
                        + "    -- 这里会自动推断表结构，生产环境建议显式定义字段\n"
                        + "    -- id INT,\n"
                        + "    -- name STRING,\n"
                        + "    -- ...\n"
                        + ") WITH (\n"
                        + "    'connector' = 'sqlserver-cdc',\n"
                        + "    'hostname' = '%s',\n"
                        + "    'port' = '%d',\n"
                        + "    'username' = '%s',\n"
                        + "    'password' = '%s',\n"
                        + "    'database-name' = '%s',\n"
                        + "    'schema-name' = '%s',\n"
                        + "    'table-name' = '%s',\n"
                        + "    'scan.incremental.snapshot.enabled' = 'true',\n"
                        + "    'scan.incremental.snapshot.chunk.size' = '8096',\n"
                        + "    'debezium.log.mining.strategy' = 'online_catalog',\n"
                        + "    'debezium.log.mining.continuous.mine' = 'true'\n"
                        + ")",
                tableName.toLowerCase(),
                sqlServerHost, sqlServerPort, sqlServerUser, sqlServerPassword,
                sqlServerDatabase, sqlServerSchema, tableName);

        // 2. 创建Paimon目标表
        String sinkDDL = String.format(
                "CREATE TABLE paimon_sink_%s (\n"
                        + "    -- 字段应与源表一致\n"
                        + "    -- id INT,\n"
                        + "    -- name STRING,\n"
                        + "    -- ...\n"
                        + "    PRIMARY KEY (id) NOT ENFORCED\n"  // 假设id是主键
                        + ") WITH (\n"
                        + "    'connector' = 'paimon',\n"
                        + "    'path' = '%s/%s/%s',\n"  // 路径格式: warehouse/database/schema.table
                        + "    'auto-create' = 'true',\n"
                        + "    'merge-engine' = 'deduplicate',\n"  // 或 'partial-update'
                        + "    'changelog-producer' = 'full-compaction',\n"
                        + "    'changelog-producer.compaction-interval' = '1 min',\n"
                        + "    'snapshot.time-retained' = '1 h'\n"
                        + ")",
                tableName.toLowerCase(),
                paimonWarehousePath, sqlServerDatabase, sqlServerSchema + "." + tableName);

        // 3. 执行DDL
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // 4. 执行同步
        tEnv.executeSql(String.format(
                "INSERT INTO paimon_sink_%s SELECT * FROM sqlserver_source_%s",
                tableName.toLowerCase(), tableName.toLowerCase()));
    }
}