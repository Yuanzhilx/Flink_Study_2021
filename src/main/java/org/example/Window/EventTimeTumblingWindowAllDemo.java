package org.example.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/***********************************
 *@Desc TODO
 *@ClassName EventTimeTumblingWindowAllDemo
 *@Author DLX
 *@Data 2021/4/21 16:10
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class EventTimeTumblingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1609430400000,1
        //1609430403000,2
        //1609430404400,1
        //1609430404999,3
        //5S窗口时间范围为[1609430400000,1609430405000)
        //当前分区中数据的携带最大EventTime - 乱序延迟时间 >= 窗口结束时间 就会触发该窗口
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        //提取数据中的时间，将时间转成精确到毫秒的Long类型，并生成WaterMark，调用该方法前后不会对数据格式产生影响
        SingleOutputStreamOperator<String> dataWithWaterMark = socketStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override//element就是我们传入的数据
            public long extractTimestamp(String element) {
                //提取数据中的时间
                return Long.parseLong(element.split(",")[0]);
            }
        });
        SingleOutputStreamOperator<Integer> nums = dataWithWaterMark.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s.split(",")[1]);
            }
        });
        SingleOutputStreamOperator<Integer> sum = nums.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))).sum(0);
        sum.print();
        env.execute("");
    }
}
