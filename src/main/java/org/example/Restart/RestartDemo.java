package org.example.Restart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.HashMap;

/***********************************
 *@Desc TODO
 *@ClassName RestartDemo
 *@Author DLX
 *@Data 2021/8/24 11:27
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class RestartDemo {
    public static void main(String[] args) throws Exception {
        //创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建DataStream
        //这表示作业最多自动重启3次，两次重启之间有5秒的延迟。
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));
        //这表示在30秒的时间内，重启次数小于3次时，继续重启，否则认定该作业为失败。两次重启之间的延迟为3秒。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30),Time.seconds(3)));
        env.setRestartStrategy(RestartStrategies.noRestart());
//        env.enableCheckpointing(10000);
        //Source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //调用Transformation
        SingleOutputStreamOperator<String> wordDataStream = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    if ("error".equals(word)){
                        throw new RuntimeException("出现异常！");
                    }
                    collector.collect(word);
                }
            }
        });

        wordDataStream.print();
        env.execute("StreamingWordCount");
    }
}
