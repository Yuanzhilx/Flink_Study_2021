package org.example.Transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName KeyDemo
 *@Author DLX
 *@Data 2021/3/29 19:39
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KeyDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
//        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy("f0");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyed.sum(1);
        sum.print();
        env.execute("");
    }
}
