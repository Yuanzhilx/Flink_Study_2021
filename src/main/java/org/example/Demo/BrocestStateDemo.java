package org.example.Demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/***********************************
 *@Desc TODO
 *@ClassName BrocestStateDemo
 *@Author DLX
 *@Data 2021/9/14 15:54
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class BrocestStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //维度表
        DataStreamSource<String> socketStream1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> socketStream2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, String>> dicTupleStream = socketStream1.map(new RichMapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });
        MapStateDescriptor<String, String> brocastMapStateDescriptor = new MapStateDescriptor<>("dic-state", String.class, String.class);
        //返回一个广播流
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = dicTupleStream.broadcast(brocastMapStateDescriptor);
        //用户，活动，事件
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpDataStream = socketStream2.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpDataStream.keyBy(t -> Tuple2.of(t.f1, t.f2), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> process = keyedStream.connect(broadcastStream).process(new MyBroadcastProcessFunc(brocastMapStateDescriptor));
        process.print();
        env.execute("");
    }
    private static class MyBroadcastProcessFunc extends KeyedBroadcastProcessFunction<Tuple2<String,String>,Tuple3<String, String, String>,Tuple3<String, String, String>, Tuple4<String,String, String, String>>{
        private MapStateDescriptor<String, String> brocastMapStateDescriptor;
        private transient ValueState<Long> activityCountState;
        private transient ValueState<HashSet<String>> disCountState;
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("ac_count", Long.class);
            activityCountState = getRuntimeContext().getState(valueStateDescriptor);
            ValueStateDescriptor<HashSet<String>> disStateDescriptor = new ValueStateDescriptor<>("dis_count", TypeInformation.of(new TypeHint<HashSet<String>>() {}));
            disCountState = getRuntimeContext().getState(disStateDescriptor);
        }
        public MyBroadcastProcessFunc() {
        }

        public MyBroadcastProcessFunc(MapStateDescriptor<String, String> brocastMapStateDescriptor) {
            this.brocastMapStateDescriptor = brocastMapStateDescriptor;
        }
        //处理输入的每一条活动数据
        @Override
        public void processElement(Tuple3<String, String, String> value, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
            ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(brocastMapStateDescriptor);
            //计算次数
            Long historyCount = activityCountState.value();
            if (historyCount == null){
                historyCount = 0L;
            }
            activityCountState.update(historyCount += 1);
            //计算人数
            HashSet<String> disUserSet = disCountState.value();
            if (disUserSet == null){
                disUserSet = new HashSet<>();
            }
            disUserSet.add(value.f0);
            disCountState.update(disUserSet);
            String eventId = value.f2;
            String eventName = broadcastState.get(eventId);
            out.collect(Tuple4.of(value.f1, eventName, activityCountState.value()+"", disCountState.value().size()+""));
        }
        //处理输入的每一条维度数据
        @Override
        public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
            String type = value.f0;
            String actId = value.f1;
            String actName = value.f2;
            //将广播的状态存起来或更新删除
            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(brocastMapStateDescriptor);
            if ("DELETE".equals(type)){
                broadcastState.remove(actId);
            }else {
                broadcastState.put(actId,actName);
            }
        }
    }
}
