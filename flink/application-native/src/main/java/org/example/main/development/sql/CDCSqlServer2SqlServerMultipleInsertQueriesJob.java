package org.example.main.development.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/7/28 17:22
 */
public class CDCSqlServer2SqlServerMultipleInsertQueriesJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册一个 "Orders" 源表，和多个 Insert 语句 结果表
        // CDC Source Tables Without primary keys - scan.incremental.snapshot.chunk.key-column
        String sourceSQLStatement = "CREATE TABLE Orders (\n" +
                "                                  id BIGINT,\n" +
                "                                  username BIGINT,\n" +
                "                                  product VARCHAR(64),\n" +
                "                                  amount INT,\n" +
                "                                  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "      'connector' = 'sqlserver-cdc',\n" +
                "      'hostname' = '192.168.138.15',\n" +
                "      'port' = '14330',\n" +
                "      'username' = 'SA',\n" +
                "      'password' = 'YourStrong!Passw0rd',\n" +
                "      'database-name' = 'TestDB',\n" +
                "      'table-name' = 'dbo.orders'\n" +
                "      );";
        tEnv.executeSql(sourceSQLStatement);

        // Connector 'sqlserver-cdc' can only be used as a source. It cannot be used as a sink.
        String sinkSQLStatement1 = "CREATE TABLE RubberOrders (\n" +
                "                                  id BIGINT,\n" +
                "                                  username BIGINT,\n" +
                "                                  product VARCHAR(64),\n" +
                "                                  amount INT,\n" +
                "                                  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "      'connector' = 'jdbc',\n" +
                "      'url' = 'jdbc:sqlserver://192.168.138.15:14330;database=TestDB',\n" +
                "      'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver',\n" +
                "      'username' = 'SA',\n" +
                "      'password' = 'YourStrong!Passw0rd',\n" +
                "      'table-name' = 'TestDB.dbo.rubber_orders'\n" +
                "      );";
        tEnv.executeSql(sinkSQLStatement1);

        // Connector 'sqlserver-cdc' can only be used as a source. It cannot be used as a sink.
        //----------------------------------------------------------------------------
        // 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
        String sinkSQLStatement2 = "CREATE TABLE GlassOrders (\n" +
                "                                  id BIGINT,\n" +
                "                                  username BIGINT,\n" +
                "                                  product VARCHAR(64),\n" +
                "                                  amount INT,\n" +
                "                                  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "      'connector' = 'jdbc',\n" +
                "      'url' = 'jdbc:sqlserver://192.168.138.15:14330;database=TestDB',\n" +
                "      'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver',\n" +
                "      'username' = 'SA',\n" +
                "      'password' = 'YourStrong!Passw0rd',\n" +
                "      'table-name' = 'TestDB.dbo.glass_orders'\n" +
                "      );";
        tEnv.executeSql(sinkSQLStatement2);

        // 运行多条 INSERT 语句，将原表数据输出到多个结果表中
        StatementSet stmtSet = tEnv.createStatementSet();
        // `addInsertSql` 方法每次只接收单条 INSERT 语句
        stmtSet.addInsertSql(
                "INSERT INTO RubberOrders SELECT id, username, product, amount FROM Orders WHERE product LIKE '%Rubber%'");
        stmtSet.addInsertSql(
                "INSERT INTO GlassOrders SELECT id, username, product, amount FROM Orders WHERE product LIKE '%Glass%'");
        // 执行刚刚添加的所有 INSERT 语句
        TableResult tableResult2 = stmtSet.execute();
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult2.getJobClient().get().getJobStatus());
    }
}
