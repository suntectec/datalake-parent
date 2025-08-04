package org.example.datastream.application;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.MyParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception in thread "main" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
 * - issue temporarily resolved by package jar submitting at docker instead of windows
 *
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class SqlServerOrders2PaimonOrdersJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(SqlServerOrders2PaimonOrdersJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String warehouse, String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {

        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder<String>()
                        .hostname(sqlserver_host)
                        .port(Integer.parseInt(sqlserver_port))
                        .databaseList("inventory")
                        .tableList("INV.orders")
                        .username(sqlserver_username)
                        .password(sqlserver_password)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        env.enableCheckpointing(3000);

        // set the source parallelism to 2
        env.fromSource(
                        sqlServerSource,
                        WatermarkStrategy.noWatermarks(),
                        "SqlServerIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        // DataStream<RichCdcRecord> dataStream =
        //         env.fromElements(
        //                 RichCdcRecord.builder(INSERT)
        //                         .field("order_id", DataTypes.BIGINT(), "123")
        //                         .field("price", DataTypes.DOUBLE(), "62.2")
        //                         .build(),
        //                 // dt field will be added with schema evolution
        //                 RichCdcRecord.builder(INSERT)
        //                         .field("order_id", DataTypes.BIGINT(), "245")
        //                         .field("price", DataTypes.DOUBLE(), "82.1")
        //                         .field("dt", DataTypes.TIMESTAMP(), "2023-06-12 20:21:12")
        //                         .build());


        // Identifier identifier = Identifier.create("my_db", "T");
        // Options catalogOptions = new Options();
        // catalogOptions.set("warehouse", warehouse);
        // catalogOptions.set("s3.endpoint", s3_endpoint);
        // catalogOptions.set("s3.access-key", s3_access_key);
        // catalogOptions.set("s3.secret-key", s3_secret_key);
        // catalogOptions.set("s3.path.style.access", "true");
        // CatalogLoader catalogLoader =
        //         () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        // catalogLoader.load().createDatabase("my_db", true);
        // catalogLoader.load().createTable(identifier,
        //         org.apache.paimon.schema.Schema.newBuilder()
        //                 .column("order_id", DataTypes.BIGINT())
        //                 .column("price", DataTypes.DOUBLE())
        //                 .column("dt", DataTypes.TIMESTAMP())
        //                 .primaryKey("order_id")
        //                 .build(), true);
        //
        // Table table = catalogLoader.load().getTable(identifier);
        //
        // new RichCdcSinkBuilder(table)
        //         .forRichCdcRecord(sqlServerIncrementalSource)
        //         .identifier(identifier)
        //         .catalogLoader(catalogLoader)
        //         .build();

        env.execute("Print SqlServer Snapshot + Change Stream");
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = MyParameter.getParameter("dev", "sqlserver.host");
        String sqlserver_port = MyParameter.getParameter("dev", "sqlserver.port");
        String sqlserver_username = MyParameter.getParameter("dev", "sqlserver.username");
        String sqlserver_password = MyParameter.getParameter("dev", "sqlserver.password");

        String warehouse = MyParameter.getParameter("dev", "warehouse");
        String s3_endpoint = MyParameter.getParameter("dev", "s3.endpoint");
        String s3_access_key = MyParameter.getParameter("dev", "s3.access-key");
        String s3_secret_key = MyParameter.getParameter("dev", "s3.secret-key");

        SqlServerOrders2PaimonOrdersJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                warehouse, s3_endpoint, s3_access_key, s3_secret_key);
    }
}
