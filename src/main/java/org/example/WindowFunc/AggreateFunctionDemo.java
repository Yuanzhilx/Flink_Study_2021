package org.example.WindowFunc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
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
 *@ClassName AggreateFunctionDemo
 *@Author DLX
 *@Data 2021/5/21 9:37
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//累加当前窗口的数据并与历史数据进行累加
public class AggreateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
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
        OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-date") {};
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wondowed = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        //直接调用sum或reduce，只会聚合窗口内的函数，不会去跟历史数据进行累加
        //需求：可以在窗口内进行增量聚合，并且还可以与历史数据进行聚合
        wondowed.aggregate(new MyAggFunc(),new MyWindowFunc()).print();
        env.execute("");

    }
    private static class MyAggFunc implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer>{
        @Override//创建一个初始值
        public Integer createAccumulator() {
            return 0;
        }
        @Override//输入一条数据与初始值或中间累加结果进行聚合
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return value.f1+accumulator;
        }
        @Override//返回结果
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }
        @Override//如果使用的是非SessionWindow可以不实现
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }
    public static class MyWindowFunc extends ProcessWindowFunction<Integer,Tuple2<String, Integer>,String,TimeWindow> {
        private transient ValueState<Integer> sumState;
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("wc_count",Integer.class);
            sumState = getRuntimeContext().getState(stateDescriptor);
        }
        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer historyCount = sumState.value();
            if (historyCount == null){
                historyCount = 0;
            }
            //获取到窗口聚合后的结果，一个key对应一条
            Integer windowCount = elements.iterator().next();
            Integer totalCount = historyCount + windowCount;
            sumState.update(totalCount);
            out.collect(Tuple2.of(key,totalCount));
        }
    }
}
