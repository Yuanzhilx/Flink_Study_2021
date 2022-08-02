package org.example.Transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName ReduceDemo
 *@Author DLX
 *@Data 2021/8/12 11:05
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> splitStream = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Tuple3.of(s1[0], s1[1], Integer.parseInt(s1[2]));
            }
        });
        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyed = splitStream.keyBy(0, 1);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> reduceStream = keyed.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> t1, Tuple3<String, String, Integer> t2) throws Exception {
                t1.f2 = t1.f2 + t2.f2;
                return t1;
            }
        });
        reduceStream.print();
        env.execute("");

    }
}
