package org.example.datastream.tableapi;

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
public class TableAPISqlServerSourceJob {
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

        TableResult tableResult = tableEnv.from("orders").groupBy($("supplier_id"))
                .select($("supplier_id"), $("item_id").count().as("cnt")).execute();
        tableResult.print();
        // No operators defined in streaming topology. Cannot execute.
        // 已经存在tableEnv.executeSql 或者 statementSet.execute() 时就不需要再 env.execute() 了！
        //        env.execute();
    }
}
