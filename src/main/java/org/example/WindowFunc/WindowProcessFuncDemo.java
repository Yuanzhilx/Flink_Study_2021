package org.example.WindowFunc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/***********************************
 *@Desc TODO
 *@ClassName WindowProcessFuncDemo
 *@Author DLX
 *@Data 2021/5/21 10:45
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class WindowProcessFuncDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        //调用该方法前后不会对数据格式产生影响
        SingleOutputStreamOperator<String> dataWithWaterMark = socketStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, timeStamp) -> Long.parseLong(line.split(",")[0])));
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = dataWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wondowed = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        wondowed.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            //窗口触发才会调用process方法，该方法回获取全量窗口数据，数据缓存在WindowState当中
            @Override
            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (Tuple2<String, Integer> element : elements) {
                    out.collect(element);
                }
            }
        }).print();
        env.execute("");

    }
}
