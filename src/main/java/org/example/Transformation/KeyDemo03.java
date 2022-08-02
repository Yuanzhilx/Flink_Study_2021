package org.example.Transformation;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***********************************
 *@Desc TODO
 *@ClassName KeyDemo03
 *@Author DLX
 *@Data 2021/3/29 20:09
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KeyDemo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<DataBean> splitStream = lines.map(new MapFunction<String, DataBean>() {
            @Override
            public DataBean map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return DataBean.of(s1[0], s1[1], Integer.parseInt(s1[2]));
            }
        });
        //直接使用字段名
//        KeyedStream<DataBean, Tuple> keyed = splitStream.keyBy("province","city");
        //字段拼接
//        KeyedStream<DataBean, String> keyed = splitStream.keyBy(new KeySelector<DataBean, String>() {
//            @Override
//            public String getKey(DataBean dataBean) throws Exception {
//                return dataBean.province+dataBean.city;
//            }
//        });
        //Tuple
        KeyedStream<DataBean, Tuple2<String, String>> keyed = splitStream.keyBy(new KeySelector<DataBean, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(DataBean dataBean) throws Exception {
                return Tuple2.of(dataBean.province, dataBean.city);
            }
        });
        //将分组后的数据进行reduce
        SingleOutputStreamOperator<DataBean> reduce = keyed.reduce(new ReduceFunction<DataBean>() {
            @Override
            public DataBean reduce(DataBean dataBean1, DataBean dataBean2) throws Exception {
                dataBean1.num = dataBean1.num + dataBean2.num;
                return dataBean1;
            }
        });
        //Lambda
        //KeyedStream<DataBean, String> keyed = splitStream.keyBy((KeySelector<DataBean, String>) dataBean -> dataBean.province);
        //检查是否为POJO类
        System.out.println(TypeInformation.of(DataBean.class).createSerializer(new ExecutionConfig()));
        reduce.print();
        env.execute("");
    }
    public static class DataBean{
        public String province;
        public String city;
        public Integer num;

        public DataBean() {
        }

        public DataBean(String province, String city, Integer num) {
            this.province = province;
            this.city = city;
            this.num = num;
        }

        private static DataBean of(String province, String city, Integer num){
            return new DataBean(province, city, num);
        }

        @Override
        public String toString() {
            return "DataBean{" +
                    "province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    ", num=" + num +
                    '}';
        }
    }
}
