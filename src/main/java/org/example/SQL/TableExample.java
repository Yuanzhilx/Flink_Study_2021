package org.example.SQL;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***********************************
 *@Desc TODO
 *@ClassName TableExample
 *@Author DLX
 *@Data 2021/3/30 19:20
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//尚硅谷教程
public class TableExample {
//    private final static Logger log = LoggerFactory.getLogger(TableExample.class);
    public static void main(String[] args) throws Exception {
//        log.error("TableExample");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStreamSource<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");
        //转换pojo
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> dataStream = inputStream.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple3.of(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        //3.创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //4.基于数据流创建表
        Table dataTbale = tableEnv.fromDataStream(dataStream);
        //5.调用table API 做操作
        Table resultTable = dataTbale.select ("f0,f1").where("f0 = 'sensor_1'");
        //6.执行SQL
        tableEnv.createTemporaryView("sensor",dataTbale);
        String sql = "select * from sensor where f0 = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("resultSqlTable");

        env.execute("TableExample");
    }
}
