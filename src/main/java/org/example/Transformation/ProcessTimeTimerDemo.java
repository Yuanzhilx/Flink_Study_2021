package org.example.Transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName ProcessTimeTimerDemo
 *@Author DLX
 *@Data 2021/8/30 11:35
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ProcessTimeTimerDemo {
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
        SingleOutputStreamOperator<Tuple2<String, Double>> processStream = keyedStream.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple2<String, Double>>() {
            private transient ValueState<Double> counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Double> wc_state = new ValueStateDescriptor<>("wc_state", Double.class);
                counter = getRuntimeContext().getState(wc_state);
            }

            @Override
            public void processElement(Tuple3<String, String, Double> value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                //获取当前的ProcessingTime
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                //获取当前时间下一个整分钟
                long fireTime = currentProcessingTime - currentProcessingTime % 60000 + 60000;
                //如果注册相同数据的TimeTimer，后面注册的会将前面注册的覆盖，即相同的定时器只会触发一次
                ctx.timerService().registerProcessingTimeTimer(fireTime);
                Double currentCount = value.f2;
                Double historyCount = counter.value();
                if (historyCount == null) {
                    historyCount = 0d;
                }
                Double totalCount = historyCount + currentCount;
                //更新状态
                counter.update(totalCount);
            }

            //当闹钟到了指定的时间就会执行onTimer
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                Double value = counter.value();
                //valueState中只保存Value不保存key
                String currentKey = ctx.getCurrentKey();
                //如果想实现只保留这一分钟数据不保留历史数据，类似滚动窗口
                //counter.update(0d);
                out.collect(Tuple2.of(currentKey, value));
            }
        });
        processStream.print();
        env.execute("");
    }
}
