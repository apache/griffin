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
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.measure.entity.AbstractAuditableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

@Entity
public class JobDataSegment extends AbstractAuditableEntity {

	private static final long serialVersionUID = -9056531122243340484L;

	private static final Logger LOGGER = LoggerFactory.getLogger(JobDataSegment.class);

    @NotNull
    private String dataConnectorName;

    private boolean baseline = false;

    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.MERGE})
    @JoinColumn(name = "segment_range_id")
    private SegmentRange segmentRange = new SegmentRange();

    @JsonProperty("as.baseline")
    public boolean isBaseline() {
        return baseline;
    }

    @JsonProperty("as.baseline")
    public void setBaseline(boolean baseline) {
        this.baseline = baseline;
    }

    @JsonProperty("segment.range")
    public SegmentRange getSegmentRange() {
        return segmentRange;
    }

    @JsonProperty("segment.range")
    public void setSegmentRange(SegmentRange segmentRange) {
        this.segmentRange = segmentRange;
    }

    @JsonProperty("data.connector.name")
    public String getDataConnectorName() {
        return dataConnectorName;
    }

    @JsonProperty("data.connector.name")
    public void setDataConnectorName(String dataConnectorName) {
        if (StringUtils.isEmpty(dataConnectorName)) {
            LOGGER.warn(" Data connector name is invalid. Please check your connector name.");
            throw new NullPointerException();
        }
        this.dataConnectorName = dataConnectorName;
    }

    public JobDataSegment() {
    }

    public JobDataSegment(String dataConnectorName, boolean baseline) {
        this.dataConnectorName = dataConnectorName;
        this.baseline = baseline;
    }

    public JobDataSegment(String dataConnectorName, boolean baseline, SegmentRange segmentRange) {
        this.dataConnectorName = dataConnectorName;
        this.baseline = baseline;
        this.segmentRange = segmentRange;
    }
}
