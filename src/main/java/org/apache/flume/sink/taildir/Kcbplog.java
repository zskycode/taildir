package org.apache.flume.sink.taildir;

/**
 * Created by zhuzhigang on 2017/7/6.
 */
public class Kcbplog {
    private String req;
    private String time;
    private String rep;
    private String millTime;

    public void init(String reqstr, String time, String rep, String millTime) {
        this.req = reqstr;
        this.time = time;
        this.rep = rep;
        this.millTime = millTime;
    }

    public String getReq() {
        return req;
    }

    public void setReq(String req) {
        this.req = req;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getRep() {
        return rep;
    }

    public void setRep(String rep) {
        this.rep = rep;
    }

    public String getMillTime() {
        return millTime;
    }

    public void setMillTime(String millTime) {
        this.millTime = millTime;
    }
}

