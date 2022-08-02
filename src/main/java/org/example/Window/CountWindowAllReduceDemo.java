package org.example.Window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/***********************************
 *@Desc TODO
 *@ClassName CountWindowAllReduceDemo
 *@Author DLX
 *@Data 2021/4/13 19:57
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class CountWindowAllReduceDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> mapStream = socketStream.map(Integer::parseInt);
        //划分Non-Keyed countWindowAll，并行度为1
        AllWindowedStream<Integer, GlobalWindow> windowAll = mapStream.countWindowAll(5);
        //把窗口数据进行增量聚合，每次数据流入都会计算结果，内存中只保留中间状态，效率更高更节省资源。
        SingleOutputStreamOperator<Integer> reduce = windowAll.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer t1, Integer t2) throws Exception {
                return t1 + t2;
            }
        });
        reduce.print();
        env.execute("");
    }
}
