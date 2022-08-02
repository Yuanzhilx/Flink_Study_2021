package org.example.Source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

/***********************************
 *@Desc TODO
 *@ClassName CustomSource03
 *@Author DLX
 *@Data 2021/3/22 19:39
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class CustomSource03 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> mySource = env.addSource(new MySource3());
        System.out.println("MySource3创建的DataStream并行度为："+mySource.getParallelism());
        mySource.print();
        env.execute("");
    }
    //继承RichParallelSourceFunction的Source是多并行的Source
    //Rich系列方法执行顺序：
    //1.调用MySource的构造方法
    //2.调用open方法仅调用一次
    //3.调用run方法开始产生数据
    //4.调用cancel方法停止run方法
    //5.调用close方法释放资源
    public static class MySource3 extends RichParallelSourceFunction<String> {
        private boolean flag = true;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("SubTask:"+getRuntimeContext().getIndexOfThisSubtask()+" Open MySource!");
        }
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (flag){
                sourceContext.collect(UUID.randomUUID().toString());
                Thread.sleep(20000);
            }
        }
        @Override
        public void cancel() {
            flag = false;
            System.out.println("SubTask:"+getRuntimeContext().getIndexOfThisSubtask()+" Cancel MySource!");
        }
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("SubTask:"+getRuntimeContext().getIndexOfThisSubtask()+" Close MySource!");
        }
    }

}
