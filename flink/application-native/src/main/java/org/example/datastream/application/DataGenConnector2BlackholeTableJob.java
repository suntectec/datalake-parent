package org.example.datastream.application;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Random;

public class DataGenConnector2BlackholeTableJob {
    public static void main(String[] args) {
        // 1. 创建表环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Create a EnvironmentSettings
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();

        // Create a TableEnvironment for batch or streaming execution.
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 2. 注册随机国家UDF
        tEnv.createTemporaryFunction("get_random_country", new RandomCountryFunction());

        // Create a source table
        tEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("cID", DataTypes.BIGINT())
                        .column("cName", DataTypes.STRING())
                        .column("cCountry", DataTypes.STRING())
                        .column("revenue", DataTypes.FLOAT())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                // 配置cID字段为序列
                .option("fields.cID.kind", "sequence")
                .option("fields.cID.start", "1")
                .option("fields.cID.end", "1000")
                // 配置cName字段为随机字符串
                .option("fields.cName.kind", "random")
                .option("fields.cName.length", "10")
                // 配置cCountry字段为从固定值中随机选择
                .option("fields.cCountry.kind", "random")
                .option("fields.cCountry.length", "4")
                // 配置revenue字段为随机浮点数
                .option("fields.revenue.kind", "random")
                .option("fields.revenue.min", "10.0")
                .option("fields.revenue.max", "10000.0")
                .build());


        // Create a sink table (using SQL DDL)
        tEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ");

        // Create a Table object from a Table API query
        Table table1 = tEnv.from("SourceTable");

        // Create a Table object from a SQL query
        Table sourceTable = tEnv.sqlQuery("SELECT *,get_random_country() AS cCountry FROM SourceTable");
        // sourceTable.execute().print();

        // 3. 执行查询并将结果插入到 SinkTable
        // // Emit a Table API result Table to a TableSink, same for SQL result
        // TableResult tableResult = table1.insertInto("SinkTable").execute();
        //
        // tEnv.executeSql(
        //         "INSERT INTO SinkTable " +
        //                 "SELECT cID, cName, SUM(revenue) AS revSum " +
        //                 "FROM SourceTable " +
        //                 "GROUP BY cID, cName"
        // );
        //
        // System.out.println(tableResult.getJobClient().get().getJobStatus());
    }

    // 优化的随机国家函数实现
    public static class RandomCountryFunction extends ScalarFunction {
        private static final String[] COUNTRIES = {
                "USA", "UK", "France", "Germany",
                "China", "Japan", "Brazil", "Australia",
                "Canada", "India", "Italy", "Spain"
        };

        private static final Random RANDOM = new Random();

        // Flink 默认会将无参数的 UDF 优化为常量。以下是几种解决方案：声明函数为非确定性函数
        // 关键：声明为非确定性函数
        @Override
        public boolean isDeterministic() {
            return false;
        }

        public String eval() {
            return COUNTRIES[RANDOM.nextInt(COUNTRIES.length)];
        }
    }
}
