package org.example.Transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/***********************************
 *@Desc TODO
 *@ClassName FilterDemo
 *@Author DLX
 *@Data 2021/3/24 16:10
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
//        SingleOutputStreamOperator<String> filterStream = lines.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return s.startsWith("a");
//            }
//        });
//        filterStream.print();

//        SingleOutputStreamOperator<String> filterStream = lines.filter(word -> word.startsWith("a"));
//        filterStream.print();
//
        SingleOutputStreamOperator<String> myFilter = lines.transform("MyFilter", TypeInformation.of(String.class), new MyStreamFilter());
        myFilter.print();

        env.execute("");
    }
    //AbstractStreamOperator<指定输出的类型>,OneInputStreamOperator<指定输入的类型，输出的类型>
    public static class MyStreamFilter extends AbstractStreamOperator<String> implements OneInputStreamOperator<String,String> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String inStr = element.getValue();
            if (inStr.startsWith("a")){
                output.collect(element.replace(inStr));
            }
        }
    }
}
