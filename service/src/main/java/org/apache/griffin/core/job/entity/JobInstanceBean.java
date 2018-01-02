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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

@Entity
public class JobInstanceBean extends AbstractAuditableEntity {

    private static final long serialVersionUID = -4748881017029815874L;

    private Long sessionId;

    @Enumerated(EnumType.STRING)
    private State state;

    private String appId;

    @Column(length = 10 * 1024)
    private String appUri;

    @Column(name = "timestamp")
    private Long tms;

    @Column(name = "expire_timestamp")
    private Long expireTms;

    private String predicateGroupName;

    private String predicateJobName;

    @Column(name = "job_deleted")
    private Boolean deleted = false;

    public Long getSessionId() {
        return sessionId;
    }

    public void setSessionId(Long sessionId) {
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

    public Long getTms() {
        return tms;
    }

    public void setTms(Long tms) {
        this.tms = tms;
    }

    public Long getExpireTms() {
        return expireTms;
    }

    public void setExpireTms(Long expireTms) {
        this.expireTms = expireTms;
    }

    public String getPredicateGroupName() {
        return predicateGroupName;
    }

    public void setPredicateGroupName(String predicateGroupName) {
        this.predicateGroupName = predicateGroupName;
    }

    public String getPredicateJobName() {
        return predicateJobName;
    }

    public void setPredicateJobName(String predicateJobName) {
        this.predicateJobName = predicateJobName;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public JobInstanceBean() {
    }

    public JobInstanceBean(State state, String pJobName, String pGroupName, Long tms, Long expireTms) {
        this.state = state;
        this.predicateJobName = pJobName;
        this.predicateGroupName = pGroupName;
        this.tms = tms;
        this.expireTms = expireTms;
    }

    public JobInstanceBean(Long sessionId, State state, String appId, String appUri, Long timestamp, Long expireTms) {
        this.sessionId = sessionId;
        this.state = state;
        this.appId = appId;
        this.appUri = appUri;
        this.tms = timestamp;
        this.expireTms = expireTms;
    }
}
