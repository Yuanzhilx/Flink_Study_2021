package org.example.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/***********************************
 *@Desc TODO
 *@ClassName ProcessingTimeSlidingWindowAllDemo
 *@Author DLX
 *@Data 2021/4/20 20:18
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ProcessingTimeSlidingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = socketStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        //老API
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.timeWindowAll(Time.seconds(10),Time.seconds(5)).sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = wordAndOne.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> s1, Tuple2<String, Integer> s2) throws Exception {
                s1.f1 = s1.f1 + s2.f1;
                return s1;
            }
        });
        reduce.print();
        env.execute("");
    }
}
