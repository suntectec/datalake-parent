package org.example.main.development.api.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Jagger
 * @since 2025/7/28 17:12
 */
public class JdbcSqlServer2SqlServerJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema tableSchema = Schema.newBuilder()
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
                .build();

        tableEnv.createTemporaryTable("orders", TableDescriptor.forConnector("jdbc")
                .schema(tableSchema)
                .option("table-name","inventory.INV.orders")
                .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("url","jdbc:sqlserver://192.168.138.15:1433;database=inventory;Encrypt=false;trustServerCertificate=true")
                .option("username","sa")
                .option("password","Abcd1234")
                .build());
        tableEnv.from("orders").printSchema();

        // create an output Table
        final Schema schema = Schema.newBuilder()
                .column("supplier_id", DataTypes.INT().notNull())
                .column("cnt", DataTypes.BIGINT())
                .primaryKey("supplier_id")
                .build();

        tableEnv.createTemporaryTable("SinkTable", TableDescriptor.forConnector("jdbc")
                .schema(schema)
                .option("table-name","TestDB.dbo.sink_order")
                .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("url","jdbc:sqlserver://192.168.138.15:14330;database=TestDB;Encrypt=false;trustServerCertificate=true")
                .option("username","sa")
                .option("password","YourStrong!Passw0rd")
                .build());

        // compute a result Table using Table API operators and/or SQL queries
        Table result = tableEnv.from("orders").groupBy($("supplier_id"))
                .select($("supplier_id"), $("item_id").count().as("cnt"));

        // Prepare the insert into pipeline
        TablePipeline pipeline = result.insertInto("SinkTable");

        // Print explain details
        pipeline.printExplain();

        // emit the result Table to the registered TableSink
        pipeline.execute();
    }
}
