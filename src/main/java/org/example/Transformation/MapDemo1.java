package org.example.Transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/***********************************
 *@Desc TODO
 *@ClassName Map
 *@Author DLX
 *@Data 2021/3/24 13:47
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class MapDemo1 {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
//        SingleOutputStreamOperator<String> upperWord = lines.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return s.toUpperCase();
//            }
//        });
        SingleOutputStreamOperator<String> upperWord = lines.map(new upperFunction());
//        SingleOutputStreamOperator<String> upperWord = lines.map(w -> w.toUpperCase());
        upperWord.print();
        env.execute("");
    }
    // 继承并实现MapFunction
    // 第一个泛型是输入类型，第二个泛型是输出类型
    public static class upperFunction implements MapFunction<String, String> {
        @Override
        public String map(String s) throws Exception {
            return s.toUpperCase();
        }
    }
}
