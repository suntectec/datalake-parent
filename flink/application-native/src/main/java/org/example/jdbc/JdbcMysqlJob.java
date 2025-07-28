package org.example.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Jagger
 * @since 2025/7/28 16:39
 */
public class JdbcMysqlJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DATE_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};
        String[] fieldNames = new String[]{"order_number", "order_date", "purchaser", "quantity", "product_id"};

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);

        JdbcInputFormat.JdbcInputFormatBuilder jdbcInputFormatBuilder = new JdbcInputFormat.JdbcInputFormatBuilder();
        JdbcInputFormat jdbc = jdbcInputFormatBuilder.setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.138.15:3306/inventory")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("select order_number, order_date, purchaser, quantity, product_id from orders")
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        DataStreamSource<Row> dsRow=env.createInput(jdbc);
        dsRow.print();

        env.execute();
    }
}