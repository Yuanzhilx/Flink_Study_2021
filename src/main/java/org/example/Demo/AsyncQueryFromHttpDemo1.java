//package org.example.Demo;
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.concurrent.TimeUnit;
//
//
///***********************************
// *@Desc TODO
// *@ClassName AsyncQueryFromHttpDemo1
// *@Author DLX
// *@Data 2021/5/24 20:18
// *@Since JDK1.8
// *@Version 1.0
// ***********************************/
//public class AsyncQueryFromHttpDemo1 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));
//        DataStream<String> line = env.socketTextStream("localhost", 8888);
//        String url = "http://localhost:8080/api";//异步IO发生Rest请求的地址
//        int capacity = 20;
//        AsyncDataStream.unorderedWait(
//                line,//输入的数据流
//                new HttpAsyncFunciton(url,capacity),//异步查询的Func实例
//                5000,//超时时间,超时则返回空
//                TimeUnit.MILLISECONDS,//时间单位
//                capacity//异步请求队列最大的数量，默认为100
//        )
//        env.execute("");
//    }
//}
