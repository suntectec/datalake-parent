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
 * Exception in thread "main" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
 * - issue temporarily resolved by package jar submitting at docker instead of windows
 *
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class CDCIngestionTable2PaimonS3TableJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(CDCIngestionTable2PaimonS3TableJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        // env.enableCheckpointing(60_000);

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

        Identifier identifier = Identifier.create("my_db", "T");
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", "s3://warehouse/paimon/");
        catalogOptions.set("s3.endpoint", "http://192.168.138.15:9000");
        catalogOptions.set("s3.access-key", "minioadmin");
        catalogOptions.set("s3.secret-key", "minioadmin");
        catalogOptions.set("s3.path.style.access", "true");
        CatalogLoader catalogLoader =
                () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        catalogLoader.load().createDatabase("my_db", true);
        catalogLoader.load().createTable(identifier,
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("price", DataTypes.DOUBLE())
                        .column("dt", DataTypes.TIMESTAMP())
                        .primaryKey("order_id")
                        .build(), true);

        Table table = catalogLoader.load().getTable(identifier);

        new RichCdcSinkBuilder(table)
                .forRichCdcRecord(dataStream)
                .identifier(identifier)
                .catalogLoader(catalogLoader)
                .build();

        env.execute();
    }
}
