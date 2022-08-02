package org.example.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/***********************************
 *@Desc TODO
 *@ClassName ProcessingTimeTumblingWindowDemo
 *@Author DLX
 *@Data 2021/4/20 19:51
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ProcessingTimeTumblingWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> eventTimewindow = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //Processing
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> processingwindow = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //窗口长度为1天，同时将数据转换成对应的时区的时间
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> eventTimewindow = keyedStream.window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)));
        processingwindow.sum(1).print();
        env.execute("");

    }
}
