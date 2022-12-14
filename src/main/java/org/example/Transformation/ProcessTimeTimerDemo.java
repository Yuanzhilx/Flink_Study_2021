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
                //???????????????ProcessingTime
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                //????????????????????????????????????
                long fireTime = currentProcessingTime - currentProcessingTime % 60000 + 60000;
                //???????????????????????????TimeTimer???????????????????????????????????????????????????????????????????????????????????????
                ctx.timerService().registerProcessingTimeTimer(fireTime);
                Double currentCount = value.f2;
                Double historyCount = counter.value();
                if (historyCount == null) {
                    historyCount = 0d;
                }
                Double totalCount = historyCount + currentCount;
                //????????????
                counter.update(totalCount);
            }

            //??????????????????????????????????????????onTimer
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                Double value = counter.value();
                //valueState????????????Value?????????key
                String currentKey = ctx.getCurrentKey();
                //????????????????????????????????????????????????????????????????????????????????????
                //counter.update(0d);
                out.collect(Tuple2.of(currentKey, value));
            }
        });
        processStream.print();
        env.execute("");
    }
}
