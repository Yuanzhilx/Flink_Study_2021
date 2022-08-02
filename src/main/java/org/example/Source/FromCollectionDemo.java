package org.example.Source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/***********************************
 *@Desc TODO
 *@ClassName FromCollectionDemo
 *@Author DLX
 *@Data 2021/3/9 10:38
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class FromCollectionDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //端口绑定给定一个范围，有小到大尝试使用端口，如果被占用则用下一个端口
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        //conf.setInteger("rest.port",8082);//两种方式
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //有限的数据流，数据流处理完程序自动退出，并行度为1
        DataStreamSource<Integer> lines = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        lines.print();
        env.execute("");
    }
}
