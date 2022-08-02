package org.example.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

/***********************************
 *@Desc TODO
 *@ClassName CustomSource01
 *@Author DLX
 *@Data 2021/3/22 19:14
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class CustomSource01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> mySource = env.addSource(new MySource1());
        System.out.println("mySource创建的DataStream并行度为："+mySource.getParallelism());
        mySource.print();
        env.execute("");
    }
    public static class MySource1 implements SourceFunction<String> {
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            List<String> strs = Arrays.asList("aaa", "bbb", "ccc", "ddd", "eee");
            for (String str:strs){
                //将Source的数据输出
                sourceContext.collect(str);
            }
        }
        @Override
        public void cancel() {}
    }
}
