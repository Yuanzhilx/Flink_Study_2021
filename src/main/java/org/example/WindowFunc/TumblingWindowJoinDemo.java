package org.example.WindowFunc;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/***********************************
 *@Desc TODO
 *@ClassName TumblingWindowJoinDemo
 *@Author DLX
 *@Data 2021/5/10 19:44
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class TumblingWindowJoinDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        //第一个流(左流)调用join方法关联第二个流(右流)，并且在Where方法和eyualTo方法中分别指定两个流的join条件
        //在同一个窗口内，join条件需要等值
        DataStream<Tuple5<Long, Long, String, String, String>> result = leftStream.join(rightStream).where(new KeySelector<Tuple3<Long, String, String>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, String> value) throws Exception {
                return value.f1;//将左流中的f1字段作为Join的key
            }
        }).equalTo(new KeySelector<Tuple3<Long, String, String>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, String> value) throws Exception {
                return value.f1;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new JoinFunction<Tuple3<Long, String, String>, Tuple3<Long, String, String>, Tuple5<Long, Long, String, String, String>>() {
            @Override
            public Tuple5<Long, Long, String, String, String> join(Tuple3<Long, String, String> first, Tuple3<Long, String, String> second) throws Exception {
                //如果进到join方法中，那么就意味着join条件相等并且在同一个窗口中
                return Tuple5.of(first.f0, second.f0, first.f1, first.f2, second.f2);
            }
        });
        result.print();
        env.execute("");
    }
}
