package org.example.main.dev.api.datastream;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class PaimonS32ConsoleJob {

    public static void main(String[] args) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get table from catalog
        Options catalogOptions = new Options();
        catalogOptions.set("type", "paimon");
        catalogOptions.set("warehouse", "s3://warehouse/paimon/");
        catalogOptions.set("s3.endpoint", "http://192.168.138.15:9000");
        catalogOptions.set("s3.access-key", "minioadmin");
        catalogOptions.set("s3.secret-key", "minioadmin");
        catalogOptions.set("s3.path.style.access", "true");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        Table table = catalog.getTable(Identifier.create("inventory", "orders"));

        // table = table.copy(Collections.singletonMap("scan.file-creation-time-millis", "..."));

        FlinkSourceBuilder builder = new FlinkSourceBuilder(table).env(env);

        // builder.sourceBounded(true);
        // builder.projection(...);
        // builder.predicate(...);
        // builder.limit(...);
        // builder.sourceParallelism(...);

        DataStream<Row> dataStream = builder.buildForRow();

        // use this datastream
        dataStream.executeAndCollect().forEachRemaining(System.out::println);

        // prints:
        // +I[Bob, 12]
        // +I[Alice, 12]
        // -U[Alice, 12]
        // +U[Alice, 14]
    }
}