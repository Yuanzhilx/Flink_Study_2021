package org.example.Restart;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/***********************************
 *@Desc TODO
 *@ClassName onTimer
 *@Author DLX
 *@Data 2021/5/20 9:38
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ProcessingTimeTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketTextStream = env.socketTextStream("localhost", 8888);
        //偶数标签
        OutputTag<String> oddOutputTag = new OutputTag<String>("odd") {};
        //奇数标签
        OutputTag<String> evenOutputTag = new OutputTag<String>("even") {};
        //非数字标签
        OutputTag<String> nanOutputTag = new OutputTag<String>("nan") {};
        SingleOutputStreamOperator<String> mainStream = socketTextStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    int i = Integer.parseInt(value);
                    if (i % 2 == 0) {//偶数
                        ctx.output(evenOutputTag, value);
                    } else {//奇数
                        ctx.output(oddOutputTag, value);
                    }
                } catch (NumberFormatException e) {
                    ctx.output(nanOutputTag, value);
                }
                //收集全部数据
                out.collect(value);
            }
        });
        //获取主流中的某几个流
        DataStream<String> oddSideOutput = mainStream.getSideOutput(oddOutputTag);
        DataStream<String> evenSideOutput = mainStream.getSideOutput(evenOutputTag);
//        DataStream<String> nanSideOutput = mainStream.getSideOutput(nanOutputTag);
        oddSideOutput.print("odd:");
        evenSideOutput.print("even:");
        mainStream.print("main:");
        env.execute("");
    }
}
