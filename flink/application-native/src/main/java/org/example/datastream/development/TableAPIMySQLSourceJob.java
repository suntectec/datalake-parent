package org.example.datastream.development;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Jagger
 * @since 2025/7/28 17:12
 */
public class TableAPIMySQLSourceJob {
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

        TableResult tableResult = tableEnv.from("orders").groupBy($("purchaser"))
                .select($("purchaser"), $("product_id").count().as("cnt")).execute();
        tableResult.print();
        // No operators defined in streaming topology. Cannot execute.
        // 已经存在tableEnv.executeSql 或者 statementSet.execute() 时就不需要再 env.execute() 了！
        //        env.execute();
    }
}
