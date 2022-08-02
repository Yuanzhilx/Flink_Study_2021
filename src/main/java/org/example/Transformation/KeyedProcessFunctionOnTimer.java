package org.example.Transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/***********************************
 *@Desc TODO
 *@ClassName KeyedProcessFunction
 *@Author DLX
 *@Data 2021/8/26 15:05
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KeyedProcessFunctionOnTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpDataStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });
        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = tpDataStream.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> processStream = keyedStream.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple3<String, String, Double>>() {

            @Override
            public void processElement(Tuple3<String, String, Double> value, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                //获取当前的ProcessingTime
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                //将当前currentProcessingTime + 30 s，注册一个定时器
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 30000);
                System.out.println("当前时间：" + currentProcessingTime + "定时器触发时间：" + currentProcessingTime + 30000);
            }

            //当闹钟到了指定的时间就会执行onTimer
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println("定时器" + timestamp + "执行了！");
            }
        });
        processStream.print();
        env.execute("");
    }
}
