package org.example.datastream.flink.paimon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class ReadFromMinIOS3 {
    public static void readFrom() throws Exception {
        // 初始化 s3 插件
        Configuration pluginConfiguration = new Configuration();
        pluginConfiguration.setString("s3.endpoint", "http://192.168.138.15:9000");
        pluginConfiguration.setString("s3.access-key", "minioadmin");
        pluginConfiguration.setString("s3.secret-key", "minioadmin");
        pluginConfiguration.setString("s3.path.style.access", "true");
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(pluginConfiguration);
        FileSystem.initialize(pluginConfiguration, pluginManager);

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 启用检查点，设置检查点间隔为 120 秒，检查点模式为 精准一次
        env.enableCheckpointing(120 * 1000, CheckpointingMode.EXACTLY_ONCE);
        // env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置并行度为 1
        env.setParallelism(1);
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("file:///./checkpoints");

        // 创建 FileSource 从 MinIO 中持续读取数据
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("s3://warehouse/test/test_data"))
                // Source 为有界/批的模式时，设置 Source 为持续的流模式
                .monitorContinuously(Duration.ofMillis(5))
                .build();

        // 从 Source 中读取数据
        DataStream<String> stream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "MinIO Source");

        // 输出流数据
        stream.print("output");

        // 执行程序
        env.execute("MinIO Source");
    }
}
