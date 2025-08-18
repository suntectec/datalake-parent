package org.example.realtime.demo.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/7/28 17:22
 */
public class CdcSqlserver2SqlserverJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册一个 "SourceOrders" 源表，和 "SinkOrders" 结果表
        String sourceSQLStatement = "CREATE TABLE SourceOrders (\n" +
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
        tEnv.executeSql(sourceSQLStatement);

        // Connector 'sqlserver-cdc' can only be used as a source. It cannot be used as a sink.
        String sinkSQLStatement = "CREATE TABLE SinkOrders (\n" +
                "                                  supplier_id INT,\n" +
                "                                  cnt BIGINT,\n" +
                "                                  PRIMARY KEY (supplier_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "      'connector' = 'jdbc',\n" +
                "      'url' = 'jdbc:sqlserver://192.168.138.15:14330;database=TestDB',\n" +
                "      'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver',\n" +
                "      'username' = 'SA',\n" +
                "      'password' = 'YourStrong!Passw0rd',\n" +
                "      'table-name' = 'TestDB.dbo.sink_order'\n" +
                "      );";
        tEnv.executeSql(sinkSQLStatement);

        // 运行一条 INSERT 语句，将源表的数据输出到结果表中
        TableResult tableResult1 = tEnv.executeSql(
                "INSERT INTO SinkOrders SELECT supplier_id ,count(item_id) as cnt FROM SourceOrders GROUP BY supplier_id");
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult1.getJobClient().get().getJobStatus());

        // //----------------------------------------------------------------------------
        // // 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
        // tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)");
        //
        // // 运行多条 INSERT 语句，将原表数据输出到多个结果表中
        // StatementSet stmtSet = tEnv.createStatementSet();
        // // `addInsertSql` 方法每次只接收单条 INSERT 语句
        // stmtSet.addInsertSql(
        //         "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
        // stmtSet.addInsertSql(
        //         "INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'");
        // // 执行刚刚添加的所有 INSERT 语句
        // TableResult tableResult2 = stmtSet.execute();
        // // 通过 TableResult 来获取作业状态
        // System.out.println(tableResult1.getJobClient().get().getJobStatus());
    }
}
