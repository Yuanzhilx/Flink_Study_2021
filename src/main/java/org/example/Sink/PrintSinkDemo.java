package org.example.Sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/***********************************
 *@Desc TODO
 *@ClassName PrintSinkDemo
 *@Author DLX
 *@Data 2021/3/23 9:50
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class PrintSinkDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        int parallelism = lines.getParallelism();
        System.out.println(parallelism);
        lines.print();
//        lines.print("PrintSinkDemo");
        lines.addSink(new MyPrintSink()).name("MyPrintSink");
        env.execute("SocketSource");
    }
    public static class MyPrintSink extends RichSinkFunction<String> {
        private int indexOfThisSubtask;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask()+1;
        }
        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println(indexOfThisSubtask+"> "+value);
        }
    }
}
