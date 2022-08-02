package org.example.Transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName aggregates
 *@Author DLX
 *@Data 2021/8/12 11:38
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class aggregates {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> splitStream = lines.map(new MapFunction<String, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Tuple3.of(Integer.parseInt(s1[0]), Integer.parseInt(s1[1]), Integer.parseInt(s1[2]));
            }
        });
        splitStream.keyBy(0).minBy(1,false).print();
        env.execute("");
    }
}
