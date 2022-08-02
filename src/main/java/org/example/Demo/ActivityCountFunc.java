package org.example.Demo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/***********************************
 *@Desc TODO
 *@ClassName ActivityCountFunc
 *@Author DLX
 *@Data 2021/5/21 16:58
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ActivityCountFunc extends KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>> {
    private transient ValueState<Long> activityCountState;
    private transient ValueState<HashSet<String>> disCountState;
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("ac_count", Long.class);
        activityCountState = getRuntimeContext().getState(valueStateDescriptor);
        ValueStateDescriptor<HashSet<String>> disStateDescriptor = new ValueStateDescriptor<>("dis_count", TypeInformation.of(new TypeHint<HashSet<String>>() {}));
        disCountState = getRuntimeContext().getState(disStateDescriptor);
    }
    @Override
    public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
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
        out.collect(Tuple4.of(value.f1, value.f2, activityCountState.value()+"", disCountState.value().size()+""));
    }
}
