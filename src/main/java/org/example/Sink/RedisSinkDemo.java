package org.example.Sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import scala.Int;

/***********************************
 *@Desc TODO
 *@ClassName RedisSinkDemo
 *@Author DLX
 *@Data 2021/3/23 16:39
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class RedisSinkDemo {
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPassword("Password").setDatabase(8).build();
        summed.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisExampleMapper()));
        env.execute("StreamingWordCount");
    }
    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override//写入Redis的方法，value使用HASH类型，并指定外面key的值得名称
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
        }
        @Override//指定写入Redis的value里面key的值
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }
        @Override//指定写入value里面value的值
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }

}
