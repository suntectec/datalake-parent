package org.example.sqlcreate;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

public class DataGenConnector2BlackholeTable {
    public static void main(String[] args) {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Create a EnvironmentSettings
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();

        // Create a TableEnvironment for batch or streaming execution.
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Create a source table
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

        // Create a sink table (using SQL DDL)
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ");

        // Create a Table object from a Table API query
        Table table1 = tableEnv.from("SourceTable");

        // Create a Table object from a SQL query
        Table table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable");
        // table2.execute().print();

        // Emit a Table API result Table to a TableSink, same for SQL result
        // TableResult tableResult = table1.insertInto("SinkTable").execute();

        tableEnv.executeSql(
                "INSERT INTO SinkTable " +
                        "SELECT * " +
                        "FROM SourceTable "
        );

        // tableEnv.executeSql(
        //         "INSERT INTO SinkTable " +
        //                 "SELECT cID, cName, SUM(revenue) AS revSum " +
        //                 "FROM SourceTable " +
        //                 "WHERE cCountry = 'FRANCE' " +
        //                 "GROUP BY cID, cName"
        // );
    }
}
