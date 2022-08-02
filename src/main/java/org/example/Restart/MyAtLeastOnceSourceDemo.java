package org.example.Restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName MyAtLeastOnceSourceDemo
 *@Author DLX
 *@Data 2021/5/18 13:04
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class MyAtLeastOnceSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //对状态做快照
        env.setParallelism(4);
        env.enableCheckpointing(30000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> errorStream = socketStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error")) {
                    int i = 10 / 0;
                }
                return value;
            }
        });
        DataStreamSource<String> dataStreamSource = env.addSource(new MyAtLeastOnceSource("D:\\IntelliJ_IDEA_Code_Space\\DataDemo\\input"));
        //如果不union在一起当出现异常时，上面的流会重启下面的流不会重启
        DataStream<String> union = errorStream.union(dataStreamSource);
        union.print();
        env.execute("");
    }
}
