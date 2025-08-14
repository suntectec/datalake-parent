package org.example.main.demo.api.datastream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class Row2PaimonFileJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(Row2PaimonFileJob.class);

    public static void main(String[] args) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        // env.enableCheckpointing(60_000);

        // create a changelog DataStream
        DataStream<Row> input =
                env.fromElements(
                                Row.ofKind(RowKind.INSERT, "Alice", 12),
                                Row.ofKind(RowKind.INSERT, "Bob", 5))
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"name", "age"}, Types.STRING, Types.INT));

        // get table from catalog
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", "/path/to/warehouse");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        catalog.createDatabase("my_db", true);
        catalog.createTable(
                Identifier.create("my_db", "T"), Schema.newBuilder()
                        .column("name", org.apache.paimon.types.DataTypes.STRING())
                        .column("age", org.apache.paimon.types.DataTypes.INT())
                        .build(), true);

        Table table = catalog.getTable(Identifier.create("my_db", "T"));

        DataType inputType =
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT()));
        FlinkSinkBuilder builder = new FlinkSinkBuilder(table).forRow(input, inputType);

        // set sink parallelism
        // builder.parallelism(_your_parallelism)

        // set overwrite mode
        // builder.overwrite(...)

        builder.build();
        env.execute();

    }
}
