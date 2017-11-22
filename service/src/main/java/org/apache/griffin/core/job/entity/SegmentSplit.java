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
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;

import javax.persistence.Column;
import javax.persistence.Entity;

@Entity
public class SegmentSplit extends AbstractAuditableEntity {

    private String offset;

    @Column(name = "data_range")
    private String range;

    private String dataUnit;

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }


    @JsonProperty("data.unit")
    public String getDataUnit() {
        return dataUnit;
    }

    @JsonProperty("data.unit")
    public void setDataUnit(String dataUnit) {
        this.dataUnit = dataUnit;
    }
}
