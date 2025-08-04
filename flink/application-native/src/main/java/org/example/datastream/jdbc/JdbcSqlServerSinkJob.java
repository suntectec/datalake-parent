package org.example.datastream.jdbc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSource;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

/**
 * @author Jagger
 * @since 2025/7/28 17:42
 */
public class JdbcSqlServerSinkJob {
    static class Order {
        public Order(Long id, String order_id) {
            this.id = id;
            this.order_id = order_id;
        }

        final Long id;
        final String order_id;

        @Override
        public String toString() {
            String idString = Long.toString(id);
            return "Book{" +
                    "id=" + idString +
                    ", order_id='" + order_id + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        JdbcSource<Order> jdbcSource =
                JdbcSource.<Order>builder()
                        .setTypeInformation(TypeInformation.of(Order.class))
                        .setSql("select id, order_id from inventory.INV.orders where id > ?")
                        .setDBUrl("jdbc:sqlserver://192.168.138.15:1433;database=inventory;Encrypt=false;trustServerCertificate=true")
                        .setUsername("sa")
                        .setPassword("Abcd1234")
                        .setJdbcParameterValuesProvider(
                                new JdbcGenericParameterValuesProvider(
                                        new Serializable[][]{{1001L}}))
                        .setDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .setResultExtractor(resultSet ->
                                new Order(
                                        resultSet.getLong("id"),
                                        resultSet.getString("order_id")))
                        .build();

        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(
                        JdbcSink.sink(
                                "insert into TestDB.dbo.books (id, order_id) values (?, ?)",
                                (statement, order) -> {
                                    statement.setLong(1, order.id);
                                    statement.setString(2, order.order_id);
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:sqlserver://192.168.138.15:14330;database=TestDB;Encrypt=false;trustServerCertificate=true")
                                        .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                                        .withUsername("sa")
                                        .withPassword("YourStrong!Passw0rd")
                                        .build()
                        ));

        env.execute();
    }
}
