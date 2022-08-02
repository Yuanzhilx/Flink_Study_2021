package org.example.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/***********************************
 *@Desc TODO
 *@ClassName FromParCollection
 *@Author DLX
 *@Data 2021/3/9 13:44
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class FromParCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1L, 20L), Long.class);
        System.out.println("任务并行度为："+nums.getParallelism());
        nums.print();
        env.execute("");
    }
}
