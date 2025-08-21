package org.example.realtime.demo.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/7/28 17:22
 */
public class Mysql2MysqlMultipleInsertQueriesJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册一个 "SourceOrders" 源表，和 "SinkOrders" 结果表
        String sourceSQLStatement = "CREATE TABLE Orders( order_number int,order_date date,purchaser int,quantity int,product_id int)with(" +
                "'connector'='jdbc'," +
                "'url'='jdbc:mysql://192.168.138.15:3306/inventory'," +
                "'driver'='com.mysql.cj.jdbc.Driver'," +
                "'username'='root'," +
                "'password'='123456',"+
                "'table-name'='orders'"
                +")";
        tEnv.executeSql(sourceSQLStatement);

        String sinkSQLStatement1 = "CREATE TABLE RubberOrders(purchaser int PRIMARY KEY NOT ENFORCED, quantity int)with(" +
                "'connector'='jdbc'," +
                "'url'='jdbc:mysql://192.168.138.15:3306/inventory'," +
                "'driver'='com.mysql.cj.jdbc.Driver'," +
                "'username'='root'," +
                "'password'='123456',"+
                "'table-name'='rubber_orders'"
                +")";
        tEnv.executeSql(sinkSQLStatement1);

        // 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
        String sinkSQLStatement2 = "CREATE TABLE GlassOrders(purchaser int PRIMARY KEY NOT ENFORCED, quantity int)with(" +
                "'connector'='jdbc'," +
                "'url'='jdbc:mysql://192.168.138.15:3306/inventory'," +
                "'driver'='com.mysql.cj.jdbc.Driver'," +
                "'username'='root'," +
                "'password'='123456',"+
                "'table-name'='glass_orders'"
                +")";
        tEnv.executeSql(sinkSQLStatement2);

        // 运行多条 INSERT 语句，将原表数据输出到多个结果表中
        StatementSet stmtSet = tEnv.createStatementSet();
        // `addInsertSql` 方法每次只接收单条 INSERT 语句
        stmtSet.addInsertSql(
                "INSERT INTO RubberOrders SELECT purchaser, sum(quantity) as quantity FROM Orders WHERE product_id in (102,107) GROUP BY purchaser");
        stmtSet.addInsertSql(
                "INSERT INTO GlassOrders SELECT purchaser, sum(quantity) as quantity FROM Orders WHERE product_id in (105, 106) GROUP BY purchaser");
        // 执行刚刚添加的所有 INSERT 语句
        TableResult tableResult2 = stmtSet.execute();
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult2.getJobClient().get().getJobStatus());
    }
}
