package org.example.datastream.flink.paimon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadFromMinIOS3A {
    public static void readFrom() throws Exception {
        Configuration pluginConfiguration = new Configuration();
        pluginConfiguration.setString("s3a.access-key", "minioadmin");
        pluginConfiguration.setString("s3a.secret-key", "minioadmin");
        pluginConfiguration.setString("s3a.connection.maximum", "1000");
        pluginConfiguration.setString("s3a.endpoint", "http://192.168.138.15:9000");
        pluginConfiguration.setBoolean("s3a.path.style.access", Boolean.TRUE);
        FileSystem.initialize(
                pluginConfiguration, PluginUtils.createPluginManagerFromRootFolder(pluginConfiguration));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///./checkpoints");

        final FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(), new Path("s3a://warehouse/test/test_data"))
                        .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source").print();

        env.execute();
    }
}
