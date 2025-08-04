package org.example.datastream.flink.s3;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在 config/config.yaml 中配置 MinIO 的 S3 相关参数，打包任务，提交运行
 */
public class ReadFromMinIOS3Env {
    private static final Logger LOG = LoggerFactory.getLogger(ReadFromMinIOS3Env.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read from S3 bucket
        env.readTextFile("s3://warehouse/test/test_data").print();
        env.execute("Read S3");

        // Write to S3 bucket
        // env.fromElements("abc", "def").writeAsText("s3://warehouse/test/sink", OVERWRITE);
        // env.execute("Write S3");

        // Use S3 as FsStatebackend
        // env.setStateBackend(new FsStateBackend("s3://<your-bucket>/<endpoint>"));

        // Use S3 as checkpoint storage
        // Configuration config = new Configuration();
        // config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        // config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://<your-bucket>/<endpoint>");
        // env.configure(config);
    }
}
