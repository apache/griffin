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

import org.apache.griffin.core.measure.AuditableEntity;

import javax.persistence.Entity;

/**
 * Created by xiangrchen on 5/31/17.
 */
@Entity
public class ScheduleState extends AuditableEntity {

    private static final long serialVersionUID = -4748881017029815874L;

    String groupName;
    String jobName;
    long scheduleid;
    String state;
    String appId;
    long timestamp;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public long getScheduleid() {
        return scheduleid;
    }

    public void setScheduleid(long scheduleid) {
        this.scheduleid = scheduleid;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ScheduleState() {
    }

    public ScheduleState(String groupName, String jobName, long scheduleid, String state, String appId, long timestamp) {
        this.groupName = groupName;
        this.jobName = jobName;
        this.scheduleid = scheduleid;
        this.state = state;
        this.appId = appId;
        this.timestamp = timestamp;
    }
}
