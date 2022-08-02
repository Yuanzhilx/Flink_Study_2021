package org.example.WindowFunc;

import org.apache.flink.api.common.functions.CoGroupFunction;
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
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName TumblingWindowLeftOuterJoinDemo
 *@Author DLX
 *@Data 2021/5/10 20:24
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class TumblingWindowLeftOuterJoinDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> socketStream1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> socketStream2 = env.socketTextStream("localhost", 9999);
        //提取第一个流里的EventTime
        SingleOutputStreamOperator<String> leftLines = socketStream1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                return Long.parseLong(line.split(",")[0]);
            }
        });
        //提取第二个流里的EventTime
        SingleOutputStreamOperator<String> rightLines = socketStream2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                return Long.parseLong(line.split(",")[0]);
            }
        });
        //将流数据整理为Tuple3
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
        DataStream<Tuple5<Long, String, String, Long, String>> result = leftStream.coGroup(rightStream).where(new KeySelector<Tuple3<Long, String, String>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, String> value) throws Exception {
                return value.f1;//将左流中的f1字段作为Join的key
            }
        }).equalTo(new KeySelector<Tuple3<Long, String, String>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, String> value) throws Exception {
                return value.f1;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new CoGroupFunction<Tuple3<Long, String, String>, Tuple3<Long, String, String>, Tuple5<Long, String, String, Long, String>>() {
            @Override
            public void coGroup(Iterable<Tuple3<Long, String, String>> first, Iterable<Tuple3<Long, String, String>> second, Collector<Tuple5<Long, String, String, Long, String>> collector) throws Exception {
                //当两个流满足条件就会调用coGroup方法，方法中的数据是同一个分区中的数据
                //如果第一个迭代器中有数据，第二个迭代器中也有数据说明join成功
                //如果第一个迭代器中有数据，第二个迭代器中没有数据说明是LeftJoin
                for (Tuple3<Long, String, String> f : first){
                    boolean joined = false;
                    for (Tuple3<Long, String, String> s : second){
                        joined = true;
                        collector.collect(Tuple5.of(f.f0, f.f1, f.f2, s.f0, s.f2));
                    }
                    if (!joined){
                        collector.collect(Tuple5.of(f.f0, f.f1, f.f2, null, null));
                    }
                }
            }
        });
        result.print();
        env.execute("");
    }
}
