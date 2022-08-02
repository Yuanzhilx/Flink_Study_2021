package org.example.Transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName RebalancingPartitioning
 *@Author DLX
 *@Data 2021/4/8 15:56
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class RebalancingPartitioning {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> mapDataStream = socketStream.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return indexOfThisSubtask + "->" + s;
            }
        }).setParallelism(1);
//        //使用轮询的方式将数据发送到下游(默认发送方式)
//        mapDataStream.rebalance().print("rebalance").setParallelism(4);
//        //使用轮询的方式将数据发送到相同TaskManager下游，不会跨节点跨网络发送
//        mapDataStream.rescale().print("rescale").setParallelism(4);
//        //使用随机的方式将数据发送到下游
//        mapDataStream.shuffle().print("shuffle").setParallelism(4);
//        //使用广播的方式将数据发送到下游，下游的每个subtask都会收到同一条数据
//        mapDataStream.broadcast().print("broadcast").setParallelism(4);
        //将所有数据发送给下游算子的第一个实例上
        mapDataStream.global().print("global").setParallelism(4);

        env.execute("");
    }
}
