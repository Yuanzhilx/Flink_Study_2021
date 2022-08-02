package org.example.SQL;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/***********************************
 *@Desc TODO
 *@ClassName TableTest6_TimeAndWindow
 *@Author DLX
 *@Data 2021/7/21 14:04
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class TableTest6_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 3. 转换成POJO
        DataStream<Tuple3<String,Long,Double>> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).returns(Types.TUPLE(Types.STRING,Types.LONG,Types.DOUBLE))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Double>>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Double> element) {
                        return element.f1*1000L;
                    }
                });
        //4.1定义处理时间
//        Table dataTable = tableEnv.fromDataStream(dataStream, "f0 as id, f1 as ts, f2 as temp,pt.proctime");
//        tableEnv.toAppendStream(dataTable, Row.class).print();
        //4.2定义事件时间
        Table dataTable = tableEnv.fromDataStream(dataStream, "f0 as id, f1 as ts, f2 as temp,rt.rowtime");
//        dataTable.printSchema();
//        tableEnv.toAppendStream(dataTable, Row.class).print();
        //5.窗口操作
        //5.1 tableAPI
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temp.avg,tw.end");
        //5.2 SQL
        tableEnv.createTemporaryView("sensor",dataTable);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp,tumble_end(rt,interval '10' second) " +
                "from sensor group by id,tumble(rt,interval '10' second)");
        tableEnv.toAppendStream(sqlResult,Row.class).print();
        //5.3 overWindow
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id,rt,id.count over ow,temp.avg over ow");
        Table sqlQueryResult = tableEnv.sqlQuery("select id,rt,count(id) over ow ,avg(temp) over ow " +
                "from sensor " +
                "window ow as (partition by id order by rt rows between 2 precending and current row)");


        env.execute("");
    }
}
