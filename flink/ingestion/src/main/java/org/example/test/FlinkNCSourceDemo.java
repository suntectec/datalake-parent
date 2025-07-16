package org.example.test;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description:
 * 发送端 nc -l -n -p 9999
 * 接收端 telnet localhost  9999
 */
public class FlinkNCSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.socketTextStream("dev-ds-trm01.tailb6e5ab.ts.net", 9999);

        dss.print();

        env.setParallelism(1);
        env.execute();
    }
}
