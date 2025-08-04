package org.example.datastream.development;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/7/28 17:22
 */
public class SQLCreateMySQLSourceJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sqlStatement = "create table orders( order_number int,order_date date,purchaser int,quantity int,product_id int)with(" +
                "'connector'='jdbc'," +
                "'url'='jdbc:mysql://192.168.138.15:3306/inventory'," +
                "'driver'='com.mysql.cj.jdbc.Driver'," +
                "'username'='root'," +
                "'password'='123456',"+
                "'table-name'='orders'"
                +")";
        tableEnv.executeSql(sqlStatement);
        tableEnv.executeSql("select purchaser ,count(product_id) as cnt from orders group by purchaser")

                .print();
    }
}
