package org.example.datastream.development;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Jagger
 * @since 2025/7/28 17:12
 */
public class TableAPIMySQLSinkJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema tableSchema = Schema.newBuilder()
                .column("order_number", DataTypes.INT().notNull())
                .column("order_date", DataTypes.DATE())
                .column("purchaser", DataTypes.INT())
                .column("quantity", DataTypes.INT())
                .column("product_id", DataTypes.INT())
                .primaryKey("order_number")
                .build();

        tableEnv.createTemporaryTable("orders", TableDescriptor.forConnector("jdbc")
                .schema(tableSchema)
                .option("table-name","orders")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://192.168.138.15:3306/inventory")
                .option("username","root")
                .option("password","123456")
                .build());
        tableEnv.from("orders").printSchema();

        // create an output Table
        final Schema schema = Schema.newBuilder()
                .column("purchaser", DataTypes.INT().notNull())
                .column("cnt", DataTypes.BIGINT())
                .primaryKey("purchaser")
                .build();

        tableEnv.createTemporaryTable("SinkTable", TableDescriptor.forConnector("jdbc")
                .schema(schema)
                .option("table-name","sink_orders")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://192.168.138.15:3306/inventory")
                .option("username","root")
                .option("password","123456")
                .build());

        // compute a result Table using Table API operators and/or SQL queries
        Table result = tableEnv.from("orders").groupBy($("purchaser"))
                .select($("purchaser"), $("product_id").count().as("cnt"));

        // Prepare the insert into pipeline
        TablePipeline pipeline = result.insertInto("SinkTable");

        // Print explain details
        pipeline.printExplain();

        // emit the result Table to the registered TableSink
        pipeline.execute();
    }
}
