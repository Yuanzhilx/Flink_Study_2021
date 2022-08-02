package org.example.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/***********************************
 *@Desc TODO
 *@ClassName CustomSource02
 *@Author DLX
 *@Data 2021/3/22 19:27
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class CustomSource02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> mySource = env.addSource(new MySource2());
        System.out.println("MySource2创建的DataStream并行度为："+mySource.getParallelism());
        mySource.print();
        env.execute("");
    }
    public static class MySource2 implements ParallelSourceFunction<String>{
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            sourceContext.collect("MySource");
        }
        @Override
        public void cancel() {}
    }
}
