package org.example.SQL.UDF;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/***********************************
 *@Desc TODO
 *@ClassName UdfTest3_TableFunction
 *@Author DLX
 *@Data 2021/7/29 14:19
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class UdfTest2_TableFunction {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 3. 转换成POJO
        DataStream<Tuple3<String,Long,Double>> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).returns(Types.TUPLE(Types.STRING,Types.LONG,Types.DOUBLE));
        //流转换成表
        Table sourceTable = tableEnv.fromDataStream(dataStream, "f0 as id, f1 as ts, f2 as temp,pt.proctime");
        //在环境中注册UDTF
        Split split = new Split("_");
        tableEnv.registerFunction("split",split);
        //tableAPI
        Table resultTable = sourceTable.joinLateral("split(id) as (word,length)")
                .select("id, ts, word, length");
        tableEnv.toAppendStream(resultTable, Row.class).print();
        //SQL
        tableEnv.createTemporaryView("sensor",sourceTable);
        Table resultSqlTbale = tableEnv.sqlQuery("select id, ts, word, length from sensor,lateral table(split(id)) as splitid(word, length)");
        tableEnv.toAppendStream(resultSqlTbale,Row.class).print();

        env.execute("");
    }
    //实现自定义的TableFunction<返回值类型>
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        // 定义属性，分隔符
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        //必须为public名字必须叫eval
        public void eval(String str) {
            //传入一行输出多行
            for (String s : str.split(separator)) {
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}
