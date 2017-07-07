/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/



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

//    public ScheduleResult(long id, String state, String appId, JsonObject appInfo, ArrayList<JsonObject> log) {
//        this.id = id;
//        this.state = state;
//        this.appId = appId;
//        this.appInfo = appInfo;
//        this.log = log;
//    }
}
