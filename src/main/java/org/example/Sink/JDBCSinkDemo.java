package org.example.Sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName JDBCSink
 *@Author DLX
 *@Data 2021/3/24 10:26
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class JDBCSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> wordDataStream = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
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
        env.enableCheckpointing(5000);
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
        summed.addSink(JdbcSink.sink(
                "insert into t_wordcount(word, counts) values (?,?) on duplicate key update counts = ?",
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setInt(2,t.f1);
                    ps.setInt(3,t.f1);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://59.111.93.226:4307/youdata")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("youdata")
                        .withPassword("Youdata@163")
                        .build()));

        env.execute("SocketSource");
    }
}
