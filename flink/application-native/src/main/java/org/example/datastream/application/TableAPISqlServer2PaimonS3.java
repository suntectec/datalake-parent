package org.example.datastream.application;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcSinkBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.paimon.types.RowKind.INSERT;
/**
 * @author Jagger
 * @since 2025/7/30 18:00
 */
public class TableAPISqlServer2PaimonS3 {

    // 类级别定义Logger
    private static final Logger logger = LoggerFactory.getLogger(TableAPISqlServer2PaimonS3.class);

    public static void run(String sqlServerHost, String sqlServerPort, String sqlServerUsername, String sqlServerPassword) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        env.enableCheckpointing(5000L);

        DataStream<RichCdcRecord> dataStream =
                env.fromElements(
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "123")
                                .field("price", DataTypes.DOUBLE(), "62.2")
                                .build(),
                        // dt field will be added with schema evolution
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "245")
                                .field("price", DataTypes.DOUBLE(), "82.1")
                                .field("dt", DataTypes.TIMESTAMP(), "2023-06-12 20:21:12")
                                .build());

        dataStream.print();

        Identifier identifier = Identifier.create("my_db", "T");
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", "s3://warehouse/paimon/");
        catalogOptions.set("s3.endpoint", "http://192.168.138.15:9000");
        catalogOptions.set("s3.access-key", "minioadmin");
        catalogOptions.set("s3.secret-key", "minioadmin");
        catalogOptions.set("s3.path.style.access", "true");
        CatalogLoader catalogLoader =
                () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        Table table = catalogLoader.load().getTable(identifier);

        new RichCdcSinkBuilder(table)
                .forRichCdcRecord(dataStream)
                .identifier(identifier)
                .catalogLoader(catalogLoader)
                .build();

        env.execute();
    }
}
