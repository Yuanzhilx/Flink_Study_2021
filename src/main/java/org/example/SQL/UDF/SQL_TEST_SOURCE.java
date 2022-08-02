package org.example.SQL.UDF;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/***********************************
 *@Desc TODO
 *@ClassName SQL_TEST_SOURCE
 *@Author DLX
 *@Data 2021/8/3 14:47
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class SQL_TEST_SOURCE {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration conf = parameterTool.getConfiguration();
        conf.setInteger("rest.port",8080);
        conf.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER,true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读入文件数据，得到DataStream
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 8888);

        // 3. 转换成POJO
        DataStream<Tuple3<String,Long,Double>> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).returns(Types.TUPLE(Types.STRING,Types.LONG,Types.DOUBLE));
        //流转换成表
        Table sourceTable = tableEnv.fromDataStream(dataStream, "f0 as id, f1 as ts, f2 as temp,pt.proctime");
        //SQL
        tableEnv.createTemporaryView("sensor",sourceTable);
//        Table resultSqlTbale = tableEnv.sqlQuery("select id,ts,if(REGEXP_REPLACE(id,'[0-9]','') in ('DPK','DPZ','DPL','dpk','dpl','',null),'运单','订单',null) as idtype from sensor");
//        Table resultSqlTbale = tableEnv.sqlQuery("select id,ts,CASE WHEN (REGEXP_REPLACE(id,'[0-9]','') in ('DPK','DPZ','DPL','dpk','dpl','',null) THEN '运单' ELSE '订单' END as idtype from sensor");
        Table resultSqlTbale = tableEnv.sqlQuery("select id,ts,CASE WHEN id IN ('A','B','C','dpk','dpl','') THEN '运单' ELSE '订单' END as idtype from sensor");
        tableEnv.toRetractStream(resultSqlTbale,Row.class).print();

        env.execute("");
    }
}
