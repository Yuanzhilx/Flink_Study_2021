package org.example.SQL.UDF;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName UdfTest4_TableAggregateFunction
 *@Author DLX
 *@Data 2021/7/29 15:20
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class UdfTest4_TableAggregateFunction {
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
        //在环境中注册UDTAF
        Top2 top2 = new Top2();
        tableEnv.registerFunction("top2",top2);
        //TableAPI
        Table resultTable = sourceTable.groupBy("id")
                .flatAggregate("top2(temp) as (temp,rank)")
                .select("id,temp,rank");
        tableEnv.toRetractStream(resultTable, Row.class).print();
        //暂不支持SQL方式调用
//        tableEnv.createTemporaryView("sensor",sourceTable);
//        Table resultSqlTbale = tableEnv.sqlQuery("select id,temp,rank from sensor,lateral table(top2(temp)) as topid(temp,rank) group by id");
//        tableEnv.toRetractStream(resultSqlTbale,Row.class).print();

        env.execute("");
    }
    //实现自定义的AggregateFunction<结果值，中间状态>
    public static class Top2 extends TableAggregateFunction<Tuple2<Double, Integer>, Tuple2<Double,Double>> {
        //输出结果Tuple2<温度, 排名>
        public void emitValue(Tuple2<Double, Double> acc, Collector<Tuple2<Double, Integer>> out) {
            if (acc.f0 != Double.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f0, 1));
            }
            if (acc.f1 != Double.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f1, 2));
            }
        }
        //初始化累加器
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<Double,Double>(Double.MIN_VALUE,Double.MIN_VALUE);
        }
        //必须实现一个accumulate(中间状态，传入的一条数据)方法，来数据之后更新状态
        public void accumulate(Tuple2<Double,Double> acc, Double temp){
            if (temp > acc.f0) {
                acc.f1 = acc.f0;
                acc.f0 = temp;
            } else if (temp > acc.f1) {
                acc.f1 = temp;
            }
        }
//        //对累加器的结果进行和合并merge(合并后的结果，累加器的集合)
//        public void merge(Tuple2<Double,Double> acc, java.lang.Iterable<Tuple2<Double,Double>> iterable) {
//            for (Tuple2<Double,Double> otherAcc : iterable) {
//                accumulate(acc, otherAcc.f0);
//                accumulate(acc, otherAcc.f1);
//            }
//        }
    }
}
