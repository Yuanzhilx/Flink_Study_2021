package org.example.Demo;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName ActivityCount
 *@Author DLX
 *@Data 2021/5/21 13:24
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//统计各个活动，各种事件的人数和次数
public class ActivityCount {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration conf = parameterTool.getConfiguration();
        conf.setInteger("rest.port",8081);
        conf.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER,true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //用户，活动，事件
        DataStreamSource<String> line = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpDataStream = line.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStreamlambda = tpDataStream.keyBy(t->Tuple2.of(t.f1, t.f2), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpDataStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        });
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> result = keyedStream.process(new ActivityCountFunc());
        result.print();
        env.execute("");
    }
}
