package org.example.Transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;


/***********************************
 *@Desc TODO
 *@ClassName NonKeyedProcessFunction
 *@Author DLX
 *@Data 2021/8/26 14:23
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class NonKeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    if (!word.equals("error")) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }
            }
        });
        wordAndOne.print();
        env.execute("");
    }
}
