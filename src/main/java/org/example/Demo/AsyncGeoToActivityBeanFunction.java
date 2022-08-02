package org.example.Demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
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
import java.util.function.Supplier;

/***********************************
 *@Desc TODO
 *@ClassName GeoToActivityBeanFunction
 *@Author DLX
 *@Data 2020/6/22 15:04
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//异步查询高德地图API
public class AsyncGeoToActivityBeanFunction extends RichAsyncFunction<String, ActivityLocationBean> {
//    transient避免链接被持久化
    private transient CloseableHttpAsyncClient httpAsyncClient = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化异步的HttpClient,指定3秒超时时间
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        httpAsyncClient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig)
                .build();
        httpAsyncClient.start();
    }

    @Override
    public void asyncInvoke(String s, ResultFuture<ActivityLocationBean> resultFuture) throws Exception {
        String[] fields = s.split(",");
        String uid = fields[0];
        String aid = fields[1];
        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        Double longitude = Double.parseDouble(fields[4]);
        Double latitude = Double.parseDouble(fields[5]);
        String url = "https://restapi.amap.com/v3/geocode/regeo?location="+longitude+","+latitude+"&key=4924f7ef5c86a278f5500851541cdcff&radius=1000&extensions=all";

        HttpGet httpGet = new HttpGet(url);
        //不调用回调方法，返回Future
        Future<HttpResponse> future = httpAsyncClient.execute(httpGet, null);
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    HttpResponse response = future.get();
                    String provine = null;
                    int status = response.getStatusLine().getStatusCode();
                    if (status == 200){
                        //获取请求的json字符串
                        String result = EntityUtils.toString(response.getEntity());
//                System.out.println(result);
                        //转成json对象
                        JSONObject jsonObject = JSON.parseObject(result);
                        //获取位置信息
                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                        if (regeocode != null && !regeocode.isEmpty()){
                            JSONObject address = regeocode.getJSONObject("addressComponent");
                            provine = address.getString("province");
                        }
                    }
                    return provine;
                }catch (Exception e){
                    return null;
                }
            }
        }).thenAccept((String province)->{resultFuture.complete(Collections.singleton(ActivityLocationBean.of(uid,aid,time,"null",eventType,longitude,latitude,province)));});
    }

    @Override
    public void close() throws Exception {
        super.close();
        httpAsyncClient.close();
    }
}
