package org.example.Restart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/***********************************
 *@Desc TODO
 *@ClassName MyKeyedState02
 *@Author DLX
 *@Data 2021/5/13 19:51
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KeyedStateDemo02 {
    public static void main(String[] args) throws Exception {
        //创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        //Source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //调用Transformation
        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpDataStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });
        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = tpDataStream.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> result = keyedStream.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple3<String, String, Double>>() {
            //transient表示该值不参与序列化反序列化
            private transient MapState<String, Double> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义一个状态描述器
                MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<>("kv-state", String.class, Double.class);
                //初始化或恢复历史状态
                mapState = getRuntimeContext().getMapState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, Double> value, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String city = value.f1;
                Double money = value.f2;
                Double historyMoney = mapState.get(city);
                if (historyMoney == null) {
                    historyMoney = 0.0;
                }
                Double totalMoney = historyMoney + money;
                //更新State
                mapState.put(city, totalMoney);
                value.f2 = totalMoney;
                out.collect(value);
            }
        });
        result.print();
        env.execute("StreamingWordCount");
    }
}
