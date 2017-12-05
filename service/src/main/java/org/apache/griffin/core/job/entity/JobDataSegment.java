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

import javax.persistence.*;

@Entity
public class JobDataSegment extends AbstractAuditableEntity {

    private String dataConnectorIndex;


    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "segment_range_id")
    private SegmentRange segmentRange;


    @JsonProperty("segment.range")
    public SegmentRange getSegmentRange() {
        return segmentRange;
    }

    @JsonProperty("segment.range")
    public void setSegmentRange(SegmentRange segmentRange) {
        this.segmentRange = segmentRange;
    }

    @JsonProperty("data.connector.index")
    public String getDataConnectorIndex() {
        return dataConnectorIndex;
    }

    @JsonProperty("data.connector.index")
    public void setDataConnectorIndex(String dataConnectorIndex) {
        this.dataConnectorIndex = dataConnectorIndex;
    }

    public JobDataSegment() {
    }
}
