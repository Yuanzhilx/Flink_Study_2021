package org.example.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName SocketSource
 *@Author DLX
 *@Data 2021/3/9 9:59
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class SocketSource {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        int parallelism = lines.getParallelism();
        System.out.println(parallelism);
        lines.print();
        env.execute("SocketSource");
    }
}
