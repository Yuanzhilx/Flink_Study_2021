package org.example.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName FromElementsDemo
 *@Author DLX
 *@Data 2021/3/9 13:40
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class FromElementsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        nums.print();
        env.execute("");
    }
}
