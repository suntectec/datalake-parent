package org.example.realtime.demo.api.datastream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.PropertiesUtil;

/**
 * Description:
 * 发送端 nc -l -p -k 9999 或 nc -l 9999
 * 接收端 telnet localhost  9999
 */
public class Netcat2ConsoleJob {
    public static void run(String flink_master) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStreamSource<String> dss = env.socketTextStream("dev-ds-trm01.tailb6e5ab.ts.net", 9999);
        DataStreamSource<String> dss = env.socketTextStream(flink_master, 9999);

        dss.print();

        env.setParallelism(1);
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        String flink_master = PropertiesUtil.getProperty("flink.master");
        Netcat2ConsoleJob.run(flink_master);
    }
}
