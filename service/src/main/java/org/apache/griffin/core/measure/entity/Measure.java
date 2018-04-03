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

package org.apache.griffin.core.measure.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.validation.constraints.NotNull;

@Entity
@Inheritance(strategy = InheritanceType.JOINED)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "measure.type")
@JsonSubTypes({@JsonSubTypes.Type(value = GriffinMeasure.class, name = "griffin"), @JsonSubTypes.Type(value = ExternalMeasure.class, name = "external")})
public abstract class Measure extends AbstractAuditableEntity {
    private static final long serialVersionUID = -4748881017029815714L;

    @NotNull
    protected String name;

    protected String owner;

    private String dqType;

    private String description;

    private String organization;

    private boolean deleted = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("dq.type")
    public String getDqType() {
        return dqType;
    }

    @JsonProperty("dq.type")
    public void setDqType(String dqType) {
        this.dqType = dqType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public Measure() {
    }

    public Measure(String name, String description, String organization, String owner) {
        this.name = name;
        this.description = description;
        this.organization = organization;
        this.owner = owner;
    }

    @JsonProperty("measure.type")
    public abstract String getType();
}
