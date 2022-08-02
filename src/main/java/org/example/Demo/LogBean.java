package org.example.Demo;

/***********************************
 *@Desc TODO
 *@ClassName LogBean
 *@Author DLX
 *@Data 2021/9/16 17:29
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class LogBean {
    public double longitude;
    public double latitude;
    public String province;
    public String city;


    public LogBean() {
    }

    public LogBean(double longitude, double latitude, String province, String city) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.province = province;
        this.city = city;
    }

    @Override
    public String toString() {
        return "LogBean{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }

    public static LogBean of(double longitude, double latitude, String province, String city){
        return new LogBean(longitude, latitude, province, city);
    }
}
