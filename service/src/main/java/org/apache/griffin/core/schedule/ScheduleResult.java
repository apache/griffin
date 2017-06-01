package org.apache.griffin.core.schedule;

import com.google.gson.JsonObject;

import java.util.ArrayList;

/**
 * Created by xiangrchen on 5/31/17.
 */
public class ScheduleResult {
    long id;
    String state;
    String appId;
    JsonObject appInfo;
    ArrayList<JsonObject> log;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public JsonObject getAppInfo() {
        return appInfo;
    }

    public void setAppInfo(JsonObject appInfo) {
        this.appInfo = appInfo;
    }

    public ArrayList<JsonObject> getLog() {
        return log;
    }

    public void setLog(ArrayList<JsonObject> log) {
        this.log = log;
    }

    public ScheduleResult() {
    }

    public ScheduleResult(long id, String state, String appId, JsonObject appInfo, ArrayList<JsonObject> log) {
        this.id = id;
        this.state = state;
        this.appId = appId;
        this.appInfo = appInfo;
        this.log = log;
    }
}
