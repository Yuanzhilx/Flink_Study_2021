package org.example.Transformation;

import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName IterateDemo
 *@Author DLX
 *@Data 2021/4/8 13:43
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class IterateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Long> numbers = socketStream.map(s -> Long.parseLong(s));
        //对Nums进行迭代（不停输入int的数字）
        IterativeStream<Long> iteration = numbers.iterate();
        //对迭代出来的数据进行运算
        //对输入的数据应用更新模型，即输入数据的处理逻辑
        SingleOutputStreamOperator<Long> iterationBody = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("iterate input =>" + value);
                return value -= 2;
            }
        });
        //只要满足value>0的条件就会形成一个回路，重新迭代，将前面的输出作为输入再进行一次应用更新模型
        SingleOutputStreamOperator<Long> feedback = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        iteration.closeWith(feedback);
        //对不满足的结果进行输出
        SingleOutputStreamOperator<Long> outPut = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });
        outPut.print();
        env.execute("");
    }
}