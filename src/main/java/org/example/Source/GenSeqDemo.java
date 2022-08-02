package org.example.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/***********************************
 *@Desc TODO
 *@ClassName GenSeqDemo
 *@Author DLX
 *@Data 2021/3/9 13:57
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class GenSeqDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> nums = env.generateSequence(1L, 100L);
        System.out.println(nums.getParallelism());

        nums.print();
        env.execute("");
    }
}
