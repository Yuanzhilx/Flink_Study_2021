package org.example.Restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/***********************************
 *@Desc TODO
 *@ClassName ListStateDemo2
 *@Author DLX
 *@Data 2021/5/18 12:55
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ListStateDemo2 {
    public static void main(String[] args) throws Exception {
        //创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        //Source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //调用Transformation
        SingleOutputStreamOperator<Tuple2<String, String>> tpDataStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });
        KeyedStream<Tuple2<String, String>, String> keyedStream = tpDataStream.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, List<String>>> process = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>>() {
            private transient ValueState<List<String>> listValueState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<List<String>> listValueStateDescriptor = new ValueStateDescriptor<>("lst_state", TypeInformation.of(new TypeHint<List<String>>(){}));
                listValueState = getRuntimeContext().getState(listValueStateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
                List<String> lst = listValueState.value();
                if (lst == null){
                    lst = new ArrayList<String>();
                }
                lst.add(value.f1);
                listValueState.update(lst);
                out.collect(Tuple2.of(value.f0, lst));
            }
        });
        process.print();
        env.execute("StreamingWordCount");
    }
}
