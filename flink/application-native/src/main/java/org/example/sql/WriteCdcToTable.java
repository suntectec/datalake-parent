package org.example.sql;

import com.amazonaws.services.s3.S3AccessPointResource;
import com.amazonaws.services.s3.request.S3HandlerContextKeys;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcSinkBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import static org.apache.paimon.types.RowKind.INSERT;

public class WriteCdcToTable {

    public static void writeTo() throws Exception {
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
        catalogOptions.set("type", "paimon");
        catalogOptions.set("warehouse", "s3://warehouse/paimon/");
        catalogOptions.set("s3.endpoint", "http://192.168.138.15:19000");
        catalogOptions.set("s3.access-key", "minioadmin");
        catalogOptions.set("s3.secret-key", "minioadmin");
        catalogOptions.set("s3.path.style.access", "true");
        catalogOptions.set("auto-create", "true");
        CatalogLoader catalogLoader =
                () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        catalogLoader.load().createTable(identifier, Schema.newBuilder().build(), true);
        Table table = catalogLoader.load().getTable(identifier);

        new RichCdcSinkBuilder(table)
                .forRichCdcRecord(dataStream)
                .identifier(identifier)
                .catalogLoader(catalogLoader)
                .build();

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        writeTo();
    }
}