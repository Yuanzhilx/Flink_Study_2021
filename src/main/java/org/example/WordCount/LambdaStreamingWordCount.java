package org.example.WordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/***********************************
 *@Desc TODO
 *@ClassName LambdaStreamingWordCount
 *@Author DLX
 *@Data 2021/3/5 14:09
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class LambdaStreamingWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getParallelism());
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);
        summed.print();
        env.execute("");
    }
}
















