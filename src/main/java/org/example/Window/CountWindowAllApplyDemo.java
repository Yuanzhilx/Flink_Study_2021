package org.example.Window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/***********************************
 *@Desc TODO
 *@ClassName CountWindowAllApplyDemo
 *@Author DLX
 *@Data 2021/4/15 20:01
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//不分组划分窗口然后调用apply对窗口内的数据进行处理
//就是将窗口中的数据先存起来（window state），当满足触发条件后再将状态中的数据取出来进行计算
public class CountWindowAllApplyDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> mapStream = socketStream.map(Integer::parseInt);
        //划分Non-Keyed countWindowAll，并行度为1
        AllWindowedStream<Integer, GlobalWindow> windowAll = mapStream.countWindowAll(5);
        SingleOutputStreamOperator<Integer> apply = windowAll.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
                ArrayList<Integer> lst = new ArrayList<>();
                for (Integer value : values) {
                    lst.add(value);
                }
                lst.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        //return o1 - o2;
                        return Integer.compare(o1,o2);
                    }
                });
                for (Integer i : lst) {
                    out.collect(i);
                }
//                Integer sum = 0;
//                for (Integer value : values) {
//                    sum += value;
//                }
//                out.collect(sum);
            }
        });
        apply.print().setParallelism(1);
        env.execute("");
    }
}
