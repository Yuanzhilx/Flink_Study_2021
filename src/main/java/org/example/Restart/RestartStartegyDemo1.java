package org.example.Restart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.HashMap;

/***********************************
 *@Desc TODO
 *@ClassName RestartStartegyDemo1
 *@Author DLX
 *@Data 2021/5/12 13:58
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class RestartStartegyDemo1 {
    public static void main(String[] args) throws Exception {
        //创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建DataStream
        //这表示作业最多自动重启3次，两次重启之间有5秒的延迟。
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));
        //这表示在30秒的时间内，重启次数小于3次时，继续重启，否则认定该作业为失败。两次重启之间的延迟为3秒。
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30),Time.seconds(3)));
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        env.enableCheckpointing(10000);
        //Source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //调用Transformation
        SingleOutputStreamOperator<String> wordDataStream = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    if ("error".equals(word)){
                        throw new RuntimeException("出现异常！");
                    }
                    collector.collect(word);
                }
            }
        });
        //将单词和1组合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = wordDataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            //存储中间结果的集合
            private HashMap<String, Integer> counter;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //获取当前SubTask的编号
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                File ckFile = new File("src/main/resources/myKeystate/"+indexOfThisSubtask);
                if (ckFile.exists()){
                    FileInputStream fileInputStream = new FileInputStream(ckFile);
                    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                    counter = (HashMap<String, Integer>)objectInputStream.readObject();
                }else {
                    counter = new HashMap<>();
                }
                //简化直接在当前SubTask启动一个定时器
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                       while (true){
                           try {
                               Thread.sleep(10000);
                               if (!ckFile.exists()){
                                   ckFile.createNewFile();
                               }
                               //将HashMap对象持久化到文件中
                               ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(ckFile));
                               objectOutputStream.writeObject(counter);
                               objectOutputStream.flush();
                               objectOutputStream.close();
                           } catch (Exception e) {
                               e.printStackTrace();
                           }
                       }
                    }
                }).start();
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                String word = input.f0;
                Integer count = input.f1;
                //从map中取出历史数据
                Integer historyCount = counter.get(word);
                if (historyCount == null) {
                    historyCount = 0;
                }
                int sum = historyCount + count;
                counter.put(word, sum);
                return Tuple2.of(word, sum);
            }
        });
        map.print();
        env.execute("StreamingWordCount");
    }
}
