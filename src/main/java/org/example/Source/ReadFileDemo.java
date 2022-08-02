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
//readFile创建的Source是一个多并行度的Source
public class ReadFileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\IntelliJ_IDEA_Work_Space\\Demo\\test.txt";
        //PROCESS_CONTINUOUSLY模式是一直监听指定的文件或目录，2秒钟检测一次文件是否发生变化
        DataStreamSource<String> lines = env.readFile(new TextInputFormat(null), path,
                FileProcessingMode.PROCESS_CONTINUOUSLY, 2000);
        System.out.println(lines.getParallelism());
        lines.print();
        env.execute("");
    }
}
