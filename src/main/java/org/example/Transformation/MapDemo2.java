package org.example.Transformation;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/***********************************
 *@Desc TODO
 *@ClassName MapDemo2
 *@Author DLX
 *@Data 2021/3/24 14:02
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class MapDemo2 {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //算子名称，返回类型，处理逻辑使用的还是Map的处理逻辑
//        SingleOutputStreamOperator<String> myUpperMap = lines.transform("MyMap", TypeInformation.of(String.class), new StreamMap<>(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return s.toUpperCase();
//            }
//        }));
//        myUpperMap.print();
        SingleOutputStreamOperator<String> myStreamMap = lines.transform("MyStreamMap", TypeInformation.of(String.class), new MyStreamMap());
        myStreamMap.print();
        env.execute("");
    }
    //AbstractStreamOperator<指定输出的类型>,OneInputStreamOperator<指定输入的类型，输出的类型>
    public static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String,String> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String inStr = element.getValue();
            String outStr = inStr.toUpperCase();
            //将要输出的数据放入到element里
            element.replace(outStr);
            //将结果输出
            output.collect(element);
        }
    }
}
