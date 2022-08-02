package org.example.Transformation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName unionDemo
 *@Author DLX
 *@Data 2021/4/8 11:18
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class unionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream1 = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> socketStream2 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> socketStream3 = env.socketTextStream("localhost", 9999);
        DataStream<String> unionStream = socketStream1.union(socketStream2,socketStream3);
        unionStream.print();
        env.execute("");
    }
}
