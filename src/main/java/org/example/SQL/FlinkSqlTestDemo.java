package org.example.SQL;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/***********************************
 *@Desc TODO
 *@ClassName FlinkSqlTestDemo
 *@Author DLX
 *@Data 2021/9/7 15:26
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class FlinkSqlTestDemo {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取数据
        DataStreamSource<String> inputStream = env.readTextFile("src/main/resources/test.txt");
        //3.转换Tuple格式
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> dataStream = inputStream.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple3.of(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });
        //3.创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //4.注册表
        tableEnv.createTemporaryView("test_table",dataStream);
        //6.执行SQL
        String sql = "select * from test_table where f0 = 'qwe'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);
        //7.输出结果
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("resultSqlTable");
        env.execute("TableExample");
    }
}
