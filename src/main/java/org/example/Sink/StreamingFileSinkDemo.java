package org.example.Sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/***********************************
 *@Desc TODO
 *@ClassName StreamingFileSinkDemo
 *@Author DLX
 *@Data 2021/3/24 9:49
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class StreamingFileSinkDemo {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前机器的逻辑核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        int parallelism = lines.getParallelism();
        System.out.println(parallelism);
        env.enableCheckpointing(5000);
        //滚动文件生成策略
        DefaultRollingPolicy<String,String> rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(30 * 1000L)//30秒滚动生成一个文件
                .withMaxPartSize(1024L*1024L*100)//当文件达到100M滚动生成一个文件
                .build();
        //创建StreamFileSink，数据以行格式写入
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.forRowFormat(
                new Path("D:\\IntelliJ_IDEA_Code_Space\\DataDemo\\output"),//文件存放目录
                new SimpleStringEncoder<String>("UTF-8")//文件编码
        ).withRollingPolicy(rollingPolicy)//传入文件滚动生成策略
                .build();
        lines.addSink(streamingFileSink);
        env.execute("SocketSource");
    }
}
