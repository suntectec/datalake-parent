package org.example.main.development.api.datastream;

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
public class JdbcMySQLBookEntity2MySQLJob {
    static class Book {
        public Book(Long id, String title) {
            this.id = id;
            this.title = title;
        }

        final Long id;
        final String title;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JdbcSource<Book> jdbcSource =
                JdbcSource.<Book>builder()
                        .setTypeInformation(TypeInformation.of(Book.class))
                        .setSql("select * from books where id < ? and id > ?")
                        .setDBUrl("jdbc:mysql://192.168.138.15:3306/inventory")
                        .setUsername("root")
                        .setPassword("123456")
                        .setJdbcParameterValuesProvider(
                                new JdbcGenericParameterValuesProvider(
                                        // where id < 1001 and id > 0
                                        new Serializable[][]{{1001L, 0L}}))
                        .setDriverName("com.mysql.cj.jdbc.Driver")
                        .setResultExtractor(resultSet ->
                                new Book(
                                        resultSet.getLong("id"),
                                        resultSet.getString("title")))
                        .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                // .addSink(new DiscardingSink());
                .addSink(
                        JdbcSink.sink(
                                "insert into books (id, title) values (?, ?)",
                                (statement, book) -> {
                                    statement.setLong(1, book.id);
                                    statement.setString(2, book.title);
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:mysql://192.168.138.15:3306/inventory")
                                        .withDriverName("com.mysql.cj.jdbc.Driver")
                                        .withUsername("root")
                                        .withPassword("123456")
                                        .build()
                        ));
        env.execute();
    }
}