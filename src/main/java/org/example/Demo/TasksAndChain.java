package org.example.Demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.util.Properties;

/***********************************
 *@Desc TODO
 *@ClassName TasksAndChain
 *@Author DLX
 *@Data 2021/8/18 15:33
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class TasksAndChain {
    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port",8082);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "59.111.211.35:9092,59.111.211.36:9092,59.111.211.37:9092");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", "g1");
        properties.setProperty("enable.auto.commit", "true");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "mytest",
                new SimpleStringSchema(),
                properties
        );
        DataStreamSource<String> source = env.addSource(kafkaConsumer).setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = source.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(" ");
                return Tuple2.of(split[0], Integer.parseInt(split[1]));
            }
        }).disableChaining().slotSharingGroup("map");
        SingleOutputStreamOperator<Tuple2<String, Integer>> applyStream = mapStream.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (Tuple2<String, Integer> tuple2 : input) {
                            out.collect(tuple2);
                        }
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, Integer>> filterStream = applyStream.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> integerTuple2) throws Exception {
                return integerTuple2.f0.startsWith("a");
            }
        });
        filterStream.print();
        env.execute("");
    }
}
