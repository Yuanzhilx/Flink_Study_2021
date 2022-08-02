package org.example.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/***********************************
 *@Desc TODO
 *@ClassName ProcessingTimeSessionWindowDemo
 *@Author DLX
 *@Data 2021/4/21 11:28
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ProcessingTimeSessionWindowDemo {
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
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> s) throws Exception {
                return s.f0;
            }
        });
        //EventTime
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> eventTimewindow = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(10)));
        //指定一个动态的时间间隔，根据数据的f1字段乘以1000得到，返回的是long类型
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> eventTimewindow = keyedStream.window(EventTimeSessionWindows.withDynamicGap((element) -> {
//            return element.f1 * 1000;
//        }));
        //Processing
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> processingwindow = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        //指定一个动态的时间间隔，根据数据的f1字段乘以1000得到，返回的是long类型
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> processingwindow = keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Integer>>() {
            @Override
            public long extract(Tuple2<String, Integer> element) {
                return element.f1 * 1000;
            }
        }));

        processingwindow.sum(1).print();
        env.execute("");
    }
}
