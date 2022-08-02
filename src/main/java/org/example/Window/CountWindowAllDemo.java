package org.example.Window;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/***********************************
 *@Desc TODO
 *@ClassName CountWindowAllDemo
 *@Author DLX
 *@Data 2021/4/12 20:28
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//CountWindowAll是GlobalWindow的一种
public class CountWindowAllDemo {
    public static void main(String[] args) throws Exception {
        //CountWindowAll是GlobalWindow的一种
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> mapStream = socketStream.map(Integer::parseInt);
        //划分Non-Keyed countWindowAll，并行度为1
        AllWindowedStream<Integer, GlobalWindow> windowAll = mapStream.countWindowAll(5);
        //把窗口数据进行聚合
        SingleOutputStreamOperator<Integer> sum = windowAll.sum(0);
        sum.print();
        env.execute("");
    }
}
