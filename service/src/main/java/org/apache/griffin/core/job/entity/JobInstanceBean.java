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

import com.fasterxml.jackson.annotation.JsonProperty;
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

    @Column(name = "predicate_group_name")
    private String predicateGroup;

    @Column(name = "predicate_job_name")
    private String predicateName;

    @Column(name = "predicate_job_deleted")
    private boolean deleted = false;

//    @ManyToOne(fetch = FetchType.EAGER)
//    @JoinColumn(name = "job_id",nullable = false)
//    private GriffinJob griffinJob;

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

    @JsonProperty("timestamp")
    public Long getTms() {
        return tms;
    }

    @JsonProperty("timestamp")
    public void setTms(Long tms) {
        this.tms = tms;
    }

    @JsonProperty("expireTimestamp")
    public Long getExpireTms() {
        return expireTms;
    }

    @JsonProperty("expireTimestamp")
    public void setExpireTms(Long expireTms) {
        this.expireTms = expireTms;
    }

    public String getPredicateGroup() {
        return predicateGroup;
    }

    public void setPredicateGroup(String predicateGroup) {
        this.predicateGroup = predicateGroup;
    }

    public String getPredicateName() {
        return predicateName;
    }

    public void setPredicateName(String predicateName) {
        this.predicateName = predicateName;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

//    public GriffinJob getGriffinJob() {
//        return griffinJob;
//    }
//
//    public void setGriffinJob(GriffinJob griffinJob) {
//        this.griffinJob = griffinJob;
//    }

    public JobInstanceBean() {
    }

    public JobInstanceBean(State state, String pName, String pGroup, Long tms, Long expireTms) {
        this.state = state;
        this.predicateName = pName;
        this.predicateGroup = pGroup;
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
