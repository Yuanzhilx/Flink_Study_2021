package org.example.Sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName WriteSinkDemo
 *@Author DLX
 *@Data 2021/3/23 10:29
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class WriteSinkDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        int parallelism = lines.getParallelism();
        System.out.println(parallelism);
//        lines.writeAsText("D:\\IntelliJ_IDEA_Code_Space\\DataDemo\\output");
//        lines.writeAsText("D:\\IntelliJ_IDEA_Code_Space\\DataDemo\\output", FileSystem.WriteMode.NO_OVERWRITE);
//        lines.map(new MapFunction<String, Tuple2<String,String>>() {
//            @Override
//            public Tuple2<String, String> map(String s) throws Exception {
//                return Tuple2.of(s,"1");
//            }
//        }).writeAsCsv("D:\\IntelliJ_IDEA_Code_Space\\DataDemo\\output\\writeAsCsv.csv");

        lines.writeUsingOutputFormat(new TextOutputFormat<>(new Path("D:\\IntelliJ_IDEA_Code_Space\\DataDemo\\output\\TextOutputFormat"))).setParallelism(1);
        env.execute("SocketSource");
    }
}
