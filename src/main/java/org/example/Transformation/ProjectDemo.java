package org.example.Transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/***********************************
 *@Desc TODO
 *@ClassName ProjectDemo
 *@Author DLX
 *@Data 2021/4/8 14:49
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ProjectDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> tupleStream = socketStream.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<String, Integer, Long>> collector) throws Exception {
                String[] s1 = s.split(" ");

                collector.collect(Tuple3.of(s1[0], Integer.parseInt(s1[1]), Long.parseLong(s1[2])));
            }
        });
        SingleOutputStreamOperator<Tuple2<Long, String>> project = tupleStream.project(2, 0);
        project.print();
        env.execute("");
    }
}