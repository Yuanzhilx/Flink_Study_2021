package org.example.SQL.UDF;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/***********************************
 *@Desc TODO
 *@ClassName MyUDF_Test
 *@Author DLX
 *@Data 2021/7/23 16:03
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class MyUDF_Test {
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
        //自定义标量函数，实现取id的hash值
        //tableAPI
        UdfTest1_ScalarFunction.HashCode hashCode = new UdfTest1_ScalarFunction.HashCode(23);
        //在环境中注册UDF
        tableEnv.registerFunction("hashCode",hashCode);
        Table resultTable = sourceTable.select("id,ts,hashCode(id)");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
//        rowDataStream.print();
        //SQL
        tableEnv.createTemporaryView("sensor",sourceTable);
        Table resultSqlTbale = tableEnv.sqlQuery("select id,ts,hashCode(id) from sensor");
        tableEnv.toAppendStream(resultSqlTbale,Row.class).print();

        env.execute("");
    }
    //实现自定义的scalarFunction
    public static class HashCode extends ScalarFunction {
        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        //必须为public名字必须叫eval
        public int eval(String str){
            return str.hashCode()*factor;
        }
    }
}
