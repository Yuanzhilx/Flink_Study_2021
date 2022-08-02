package org.example.Demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.OutputTag;

import java.time.Duration;

/***********************************
 *@Desc TODO
 *@ClassName SideOutPutDemo
 *@Author DLX
 *@Data 2021/8/30 19:49
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class SideOutPutDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        //使用新的API生成WaterMark调用该方法前后不会对数据格式产生影响
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
        OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-date") {};
        //EventTime
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wondowed = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lateOutputTag)//对迟到数据打上标签
                .allowedLateness(Time.seconds(2));
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wondowed.sum(1);
        //获取迟到数据
        DataStream<Tuple2<String, Integer>> lateDataStream = summed.getSideOutput(lateOutputTag);
        summed.print();
        lateDataStream.print("lateDataStream");
        env.execute("");
    }
}
