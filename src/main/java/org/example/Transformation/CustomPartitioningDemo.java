package org.example.Transformation;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName CustomPartitioningDemo
 *@Author DLX
 *@Data 2021/4/8 17:32
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class CustomPartitioningDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置IngestionTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //设置ProcessingTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String,String>> mapDataStream = socketStream.map(new RichMapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Tuple2.of(s1[0],s1[1]);
            }
        });
        DataStream<Tuple2<String,String>> partitionStream = mapDataStream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                int res = 0;
                if (key.equals("key1")){
                    res = 1;
                }else if (key.equals("key2")){
                    res = 2;
                }else if (key.equals("key3")){
                    res = 3;
                }
                return res;
            }
        }, new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> tp) throws Exception {
                return tp.f0;
            }
        });
        partitionStream.print("partitionStream");
        env.execute("");
    }
}
