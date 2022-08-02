package org.example.WindowFunc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/***********************************
 *@Desc TODO
 *@ClassName WindowLaterDataDemo
 *@Author DLX
 *@Data 2021/5/20 13:25
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class WindowLaterDataDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        //调用该方法前后不会对数据格式产生影响
        SingleOutputStreamOperator<String> dataWithWaterMark = socketStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, timeStamp) -> Long.parseLong(line.split(",")[0])));
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = dataWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wondowed = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        //直接调用sum或reduce，只会聚合窗口内的函数，不会去跟历史数据进行累加
        //需求：可以在窗口内进行增量聚合，并且还可以与历史数据进行聚合
        wondowed.reduce(new MyReduceFunc(),new MyWindowFunc()).print();
        env.execute("");

    }
    public static class MyReduceFunc implements ReduceFunction<Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            value1.f1 = value1.f1+ value2.f1;
            return value1;
        }
    }
    //对窗口聚合后的数据进行汇总
    public static class MyWindowFunc extends ProcessWindowFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,String,TimeWindow> {
        private transient ValueState<Integer> sumState;
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("wc_count",Integer.class);
            sumState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer historyCount = sumState.value();
            if (historyCount == null){
                historyCount = 0;
            }
            //获取到窗口聚合后的结果，一个key对应一条
            Tuple2<String, Integer> tp = elements.iterator().next();
            Integer windowCount = tp.f1;
            tp.f1 = historyCount + windowCount;
            sumState.update(tp.f1);
            out.collect(tp);
        }
    }
}
