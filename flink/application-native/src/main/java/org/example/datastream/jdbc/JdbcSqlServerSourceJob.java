package org.example.datastream.jdbc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSource;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * @author Jagger
 * @since 2025/7/28 17:42
 */
public class JdbcSqlServerSourceJob {
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
    };

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
                .process(new ProcessFunction<Order, String>() {
                    @Override
                    public void processElement(Order order, ProcessFunction<Order, String>.Context context, Collector<String> collector) throws Exception {
                        String orderString = order.toString();
                        collector.collect(orderString);
                    }
                }).print();

        env.execute();
    }
}
