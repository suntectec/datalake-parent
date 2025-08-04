package org.example.datastream.flink.s3;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * 在 config/config.yaml 中配置 MinIO 的 S3 相关参数，打包任务，提交运行
 */
public class WriteToMinIOS3Env {
    private static final Logger LOG = LoggerFactory.getLogger(WriteToMinIOS3Env.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read from S3 bucket
        // env.readTextFile("s3://warehouse/test/test_data").print();
        // env.execute("Read S3");

        // Write to S3 bucket
        env.fromElements("abc", "def").writeAsText("s3://warehouse/test/flink/sink", OVERWRITE);
        env.execute("Write S3");

        // Use S3 as checkpoint storage
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://warehouse/test/flink/checkpoints");
        env.configure(config);
    }
}
