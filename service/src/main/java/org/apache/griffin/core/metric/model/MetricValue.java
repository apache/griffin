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

package org.apache.griffin.core.metric.model;

import java.util.Map;
import java.util.Objects;

public class MetricValue {

    private String name;

    private Long tmst;

    private String applicationId;

    private Map<String, Object> value;

    public MetricValue() {
    }

    public MetricValue(String name, Long tmst, Map<String, Object> value) {
        this.name = name;
        this.tmst = tmst;
        this.value = value;
    }


    public MetricValue(String name, Long tmst, String applicationId, Map<String, Object> value) {
        this.name = name;
        this.tmst = tmst;
        this.applicationId = applicationId;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTmst() {
        return tmst;
    }

    public void setTmst(Long tmst) {
        this.tmst = tmst;
    }

    public Map<String, Object> getValue() {
        return value;
    }

    public void setValue(Map<String, Object> value) {
        this.value = value;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricValue that = (MetricValue) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(tmst, that.tmst) &&
                Objects.equals(applicationId, that.applicationId) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tmst, applicationId, value);
    }

    @Override
    public String toString() {
        return "MetricValue{" +
                "name='" + name + '\'' +
                ", tmst=" + tmst +
                ", applicationId='" + applicationId + '\'' +
                ", value=" + value +
                '}';
    }
}
