package org.example.Source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/***********************************
 *@Desc TODO
 *@ClassName ReadFileDemo
 *@Author DLX
 *@Data 2021/3/9 15:35
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/

public class ReadTextFileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\IntelliJ_IDEA_Work_Space\\Demo";
        DataStreamSource<String> lines = env.readTextFile(path);
        System.out.println(lines.getParallelism());
        lines.print();
        env.execute("");
    }
}
