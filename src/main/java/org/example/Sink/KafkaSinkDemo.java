package org.example.Sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/***********************************
 *@Desc TODO
 *@ClassName KafkaSinkDemo
 *@Author DLX
 *@Data 2021/3/23 10:47
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        int parallelism = lines.getParallelism();
        System.out.println(parallelism);
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("BrokerList:8888", "TopicID", new SimpleStringSchema());
        lines.addSink(kafkaProducer);
        env.execute("SocketSource");
    }
}
