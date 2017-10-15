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

package org.apache.griffin.core.job.entity;

import org.apache.griffin.core.job.entity.LivySessionStates.State;
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;

import javax.persistence.*;

@Entity
public class JobInstance extends AbstractAuditableEntity {

    private static final long serialVersionUID = -4748881017029815874L;

    private String groupName;
    private String jobName;
    private int sessionId;
    @Enumerated(EnumType.STRING)
    private State state;
    private String appId;
    @Lob
    @Column(length = 1024)
    private String appUri;
    private long timestamp;

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

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppUri() {
        return appUri;
    }

    public void setAppUri(String appUri) {
        this.appUri = appUri;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public JobInstance() {
    }

    public JobInstance(String groupName, String jobName, int sessionId, State state, String appId, String appUri, long timestamp) {
        this.groupName = groupName;
        this.jobName = jobName;
        this.sessionId = sessionId;
        this.state = state;
        this.appId = appId;
        this.appUri = appUri;
        this.timestamp = timestamp;
    }
}
