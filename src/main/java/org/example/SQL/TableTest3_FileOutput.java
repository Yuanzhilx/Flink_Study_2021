package org.example.SQL;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

/**
 * @ClassName: TableTest3_FileOutput
 * @Description:
 * @Author: wushengran on 2020/11/13 11:54
 * @Version: 1.0
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        // 读取文件
        String filePath = "src/main/resources/sensor.txt";
        tableEnv.connect( new FileSystem().path(filePath))
                .withFormat( new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                .filter("id === 'sensor_1'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 4. 输出到文件
        // 连接外部文件注册输出表
        String outputPath = "src/main/resources/out1.txt";
        tableEnv.connect( new FileSystem().path(outputPath))
                .withFormat( new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("cnt", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

//        resultTable.insertInto("outputTable");
        tableEnv.sqlQuery("insert into outputTable(id,temperature) select id, temp as temperature from inputTable where id = 'senosr_6'");
//        aggTable.insertInto("outputTable");

        env.execute("");
    }
}
