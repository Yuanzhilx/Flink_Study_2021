package org.example.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/***********************************
 *@Desc TODO
 *@ClassName EventTimeSessionWindowDemo
 *@Author DLX
 *@Data 2021/4/21 20:12
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class EventTimeSessionWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        //调用该方法前后不会对数据格式产生影响
        SingleOutputStreamOperator<String> dataWithWaterMark = socketStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override//element就是我们传入的数据
            public long extractTimestamp(String element) {
                //提取数据中的时间
                return Long.parseLong(element.split(",")[0]);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = dataWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> s) throws Exception {
                return s.f0;
            }
        });
        //EventTime
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> eventTimewindow = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        eventTimewindow.sum(1).print();
        env.execute("");
    }
}
