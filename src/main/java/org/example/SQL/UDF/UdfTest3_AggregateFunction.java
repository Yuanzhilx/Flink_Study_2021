package org.example.SQL.UDF;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/***********************************
 *@Desc TODO
 *@ClassName UdfTest3_AggregateFunction
 *@Author DLX
 *@Data 2021/7/29 11:22
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class UdfTest3_AggregateFunction {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读入数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");
//        DataStream<String> inputStream = env.socketTextStream("localhost", 8888);;

        //转换成POJO
        DataStream<Tuple3<String,Long,Double>> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).returns(Types.TUPLE(Types.STRING,Types.LONG,Types.DOUBLE));

        //流转换成表 Tuple3<String,Long,Double>
        Table sourceTable = tableEnv.fromDataStream(dataStream, "f0 as id, f1 as ts, f2 as temp,pt.proctime");
        //在环境中注册UDAF
        AvgTemp avgTemp = new AvgTemp();
        tableEnv.registerFunction("avgTemp",avgTemp);
        //tableAPI
        Table resultTable = sourceTable.groupBy("id")
                .aggregate("avgTemp(temp) as avgtemp")
                .select("id,avgtemp");
//        tableEnv.toRetractStream(resultTable, Row.class).print();
        //SQL
        tableEnv.createTemporaryView("sensor",sourceTable);
        Table resultSqlTbale = tableEnv.sqlQuery("select id,avgTemp(temp) as avgtemp from sensor group by id");
        tableEnv.toRetractStream(resultSqlTbale,Row.class).print();

        env.execute("");
    }
    //实现自定义的AggregateFunction<结果值，中间状态>
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double,Integer>> {
        //输出结果
        @Override
        public Double getValue(Tuple2<Double, Integer> acc) {
            return acc.f0/acc.f1;
        }
        //初始化累加器
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<Double,Integer>(0.0,0);
        }
        //必须实现一个accumulate(中间状态，传入的一条数据)方法，来数据之后更新状态
        public void accumulate(Tuple2<Double,Integer> acc, Double temp){
            acc.f0 += temp;
            acc.f1 += 1;
        }
    }
}
