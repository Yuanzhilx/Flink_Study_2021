package org.example.Demo;

/***********************************
 *@Desc TODO
 *@ClassName ActivityBean
 *@Author DLX
 *@Data 2020/6/22 10:42
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//com.asia.dlx.Demand.QueryActivityName 中的维度信息
public class ActivityLocationBean {
    public String uid;
    public String aid;
    public String activityName;
    public String atime;
    public int eventType;
    public double longitude;
    public double latitude;
    public String province;
    public int count = 1;

    public ActivityLocationBean() {
    }

    public ActivityLocationBean(String uid, String aid, String activityName, String atime, int eventType, double longitude, double latitude, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.atime = atime;
        this.eventType = eventType;
        this.province = province;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public String toString() {
        return "ActivityLocationBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", activityName='" + activityName + '\'' +
                ", atime='" + atime + '\'' +
                ", eventType=" + eventType +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }

    public static ActivityLocationBean of(String uid, String aid, String activityName, String atime, int eventType, double longitude, double latitude, String province){
        return new ActivityLocationBean(uid, aid, activityName, atime, eventType, longitude,latitude, province);
    }

}
