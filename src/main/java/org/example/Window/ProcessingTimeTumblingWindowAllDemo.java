package org.example.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/***********************************
 *@Desc TODO
 *@ClassName ProcessingTimeTumblingWindowAllDemo
 *@Author DLX
 *@Data 2021/4/19 20:01
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ProcessingTimeTumblingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = socketStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(" ");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        //老API
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.timeWindowAll(Time.seconds(10)).sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))).sum(1);
        sum.print();
        env.execute("");
    }
}
