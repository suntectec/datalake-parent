package org.example.datastream.application;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Jagger
 * @since 2025/8/4 10:56
 */
public class ReadPaimonFileTable2ConsoleJob {
    public static void main(String[] args) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get table from catalog
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", "/path/to/warehouse");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        Table table = catalog.getTable(Identifier.create("my_db", "T"));

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
