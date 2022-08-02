package org.example.Demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;


import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/***********************************
 *@Desc TODO
 *@ClassName AsyncHttpClient
 *@Author DLX
 *@Data 2021/9/15 11:17
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class AsyncHttpClient {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //{"longitude":"120.19","latitude":"30.26"}
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        String url = "https://restapi.amap.com/v3/geocode/regeo";
        String key = "4924f7ef5c86a278f5500851541cdcff";
        int capacity = 50;//最大异步请求并发数量
        //调用orderedWait（请求按顺序返回）,或者unorderedWait（请求不按顺序返回）
        SingleOutputStreamOperator<LogBean> unorderedWait = AsyncDataStream.unorderedWait(
                lines,//输入数据流
                new AsyncHttpGeoQueryFunction(url, key, capacity),
                3000,//超时时间
                TimeUnit.MILLISECONDS,//时间单位
                capacity);//异步请求队列最大数量，默认100
        unorderedWait.print();
        env.execute("");
    }
    public static class AsyncHttpGeoQueryFunction extends RichAsyncFunction<String,LogBean> {
        private transient CloseableHttpAsyncClient httpAsyncClient;
        private String url;
        private String key;
        private int maxConnTotal;//异步httpclient支持的最大连接

        public AsyncHttpGeoQueryFunction() {
        }

        public AsyncHttpGeoQueryFunction(String url, String key, int maxConnTotal) {
            this.url = url;
            this.key = key;
            this.maxConnTotal = maxConnTotal;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //指定3S超时时间
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(3000)
                    .setConnectTimeout(3000)
                    .build();
            httpAsyncClient = HttpAsyncClients.custom()//创建Http异步请求连接池
                    .setMaxConnTotal(maxConnTotal)//设置最大连接数
                    .setDefaultRequestConfig(requestConfig).build();
            httpAsyncClient.start();//启动异步请求httpAsyncClient
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<LogBean> resultFuture) throws Exception {
            //使用fastjson将json字符串解析成json对象
            LogBean bean = JSON.parseObject(input,LogBean.class);
            double longitude = bean.longitude;//获取精度
            double latitude = bean.latitude;//获取维度
            System.out.println("longitude:"+longitude+"latitude:"+latitude);
            //将经纬度与高德地图apikey和url拼接
            String url = "https://restapi.amap.com/v3/geocode/regeo?location="+longitude+","+latitude+"&key=4924f7ef5c86a278f5500851541cdcff&radius=1000&extensions=all";
            HttpGet httpGet = new HttpGet(url);
            //发送异步请求返回Future
            Future<HttpResponse> future = httpAsyncClient.execute(httpGet, null);
            CompletableFuture.supplyAsync(new Supplier<LogBean>() {
                @Override
                public LogBean get() {
                    try {
                        HttpResponse response = future.get();
                        String province = null;
                        String city = null;
                        int status = response.getStatusLine().getStatusCode();
                        if (status == 200){
                            //获取请求的json字符串
                            String result = EntityUtils.toString(response.getEntity());
                            System.out.println(result);
                            //转成json对象
                            JSONObject jsonObject = JSON.parseObject(result);
                            //获取位置信息
                            JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                            if (regeocode != null && !regeocode.isEmpty()){
                                JSONObject address = regeocode.getJSONObject("addressComponent");
                                province = address.getString("province");
                                city = address.getString("city");
                            }
                        }
                        bean.province = province;
                        bean.city = city;
                        return bean;
                    }catch (Exception e){//如果3秒之内没有返回则返回null
                        return null;
                    }
                }
            }).thenAccept((LogBean result)->{resultFuture.complete(Collections.singleton(result));});
        }

        @Override
        public void close() throws Exception {
            super.close();
            httpAsyncClient.close();
        }
    }
}
