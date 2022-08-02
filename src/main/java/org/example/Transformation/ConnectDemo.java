package org.example.Transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import scala.Int;

/***********************************
 *@Desc TODO
 *@ClassName ConnectDemo
 *@Author DLX
 *@Data 2021/4/8 11:35
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream1 = env.socketTextStream("localhost", 8888);
        DataStream<Integer> socketStream2 = env.socketTextStream("localhost", 9999).map(s -> Integer.parseInt(s));
        ConnectedStreams<String, Integer> connectedStreams = socketStream1.connect(socketStream2);
        //两个流的map方法执行完的返回值会放入一个新的流中
        SingleOutputStreamOperator<String> coMapResult = connectedStreams.map(new CoMapFunction<String, Integer, String>() {
            @Override//对第一个流进行map的方法
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override//对第二个流进行map的方法
            public String map2(Integer value) throws Exception {
                return (value * 10) + "";
            }
        });
        SingleOutputStreamOperator<String> coFlatMapResult = connectedStreams.flatMap(new CoFlatMapFunction<String, Integer, String>() {
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                String[] str = value.split(" ");
                for (String s : str) {
                    out.collect(s.toUpperCase());
                }
            }

            @Override
            public void flatMap2(Integer value, Collector<String> out) throws Exception {
                out.collect((value * 10) + "");
            }
        });
        coFlatMapResult.print();
        env.execute("");
    }
}
