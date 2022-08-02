package org.example.Transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/***********************************
 *@Desc TODO
 *@ClassName FlatMapDemo
 *@Author DLX
 *@Data 2021/3/24 15:04
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
//        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//                String[] words = s.split(" ");
//                for (String word : words) {
//                    collector.collect(word);
//                }
//            }
//        });
//        words.print();

//        DataStream<Tuple2<String, Integer>> wordAndOne = lines.flatMap(
//                (String line, Collector<Tuple2<String, Integer>> out) -> {
//                    Arrays.asList(line.split("\\W+")).forEach(word -> {
//                        out.collect(Tuple2.of(word.toLowerCase(), 1));
//                    });
//                }
//        ).returns(Types.TUPLE(Types.STRING, Types.INT));//使用returns指定返回数据的类型
//        wordAndOne.print();

        SingleOutputStreamOperator<String> myFlatMap = lines.transform("MyFlatMap", TypeInformation.of(String.class), new MyStreamFlatMap());
        myFlatMap.print();

        env.execute("");
    }
    //AbstractStreamOperator<指定输出的类型>,OneInputStreamOperator<指定输入的类型，输出的类型>
    public static class MyStreamFlatMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String,String> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String inStr = element.getValue();
            String[] words = inStr.split(" ");
            for (String word : words) {
                output.collect(element.replace(word));
            }
        }
    }
}
