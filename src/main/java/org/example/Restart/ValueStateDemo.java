package org.example.Restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName ValueStateDemo
 *@Author DLX
 *@Data 2021/8/25 11:45
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,5000));
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //调用Transformation
        SingleOutputStreamOperator<Tuple2<String,Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = wordAndOne.keyBy(0).map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private transient ValueState<Integer> counter;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //想使用状态要先定义一个状态描述器（State的名称，类型）
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc-desc", Integer.class);
                //初始化或恢复历史状态
                counter = getRuntimeContext().getState(stateDescriptor);
            }
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                Integer currentCount = input.f1;
                //从ValueState中取出历史次数
                Integer historyCount = counter.value();//获取当前key对应的value
                if (historyCount == null) {
                    historyCount = 0;
                }
                Integer toutle = historyCount + currentCount;//累加
                //更新内存中的状态
                counter.update(toutle);
                input.f1 = toutle;
                return input;
            }
        });
        map.print();
        env.execute("");
    }
}
