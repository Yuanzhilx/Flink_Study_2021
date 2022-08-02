package org.example.WindowFunc;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName IntervalJoinDemo
 *@Author DLX
 *@Data 2021/5/11 10:51
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> socketStream1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> socketStream2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> leftLines = socketStream1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                return Long.parseLong(line.split(",")[0]);
            }
        });
        SingleOutputStreamOperator<String> rightLines = socketStream2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                return Long.parseLong(line.split(",")[0]);
            }
        });
        SingleOutputStreamOperator<Tuple3<Long, String, String>> leftStream = leftLines.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String value) throws Exception {
                String[] field = value.split(",");
                return Tuple3.of(Long.parseLong(field[0]), field[1], field[2]);
            }
        });
        SingleOutputStreamOperator<Tuple3<Long, String, String>> rightStream = rightLines.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String value) throws Exception {
                String[] field = value.split(",");
                return Tuple3.of(Long.parseLong(field[0]), field[1], field[2]);
            }
        });
        SingleOutputStreamOperator<Tuple5<Long, Long, String, String, String>> process = leftStream
                .keyBy(t -> t.f1)//指定第一个流分组KeySelector
                .intervalJoin(rightStream.keyBy(t -> t.f1))//调用intervalJoin方法并指定第二个流分组
                .between(Time.seconds(-1), Time.seconds(1))//设置join的时间区间范围为当前数据时间±1秒
                .upperBoundExclusive()//默认join的时间范围为前后都包括的闭区间，现在设置为前闭后开区间
                .process(new ProcessJoinFunction<Tuple3<Long, String, String>, Tuple3<Long, String, String>, Tuple5<Long, Long, String, String, String>>() {
                    @Override
                    public void processElement(Tuple3<Long, String, String> left, Tuple3<Long, String, String> right, Context ctx, Collector<Tuple5<Long, Long, String, String, String>> out) throws Exception {
                        //将join上的数据输出
                        out.collect(Tuple5.of(left.f0, right.f0, left.f1, left.f2, right.f2));
                    }
                });
        process.print();
        env.execute("");
    }
}
