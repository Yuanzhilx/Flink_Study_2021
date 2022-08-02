package org.example.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/***********************************
 *@Desc TODO
 *@ClassName EventTimeTumblingWindowDemo
 *@Author DLX
 *@Data 2021/4/21 16:44
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class EventTimeTumblingWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> map = socketStream.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple3.of(Long.parseLong(split[0]), split[1], Integer.parseInt(split[2]));
            }
        }).setParallelism(2);
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> timestampsAndWatermarks = map.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                return element.f0;
            }
        });
        KeyedStream<Tuple, Tuple> keyedStream = timestampsAndWatermarks.project(1, 2).keyBy(0);
        SingleOutputStreamOperator<Tuple> sum = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5))).sum(1);
        sum.print();
        env.execute("");
    }
}
