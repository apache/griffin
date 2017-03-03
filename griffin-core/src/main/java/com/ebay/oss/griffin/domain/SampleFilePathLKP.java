/*
 * Copyright (c) 2016 eBay Software Foundation. Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.ebay.oss.griffin.domain;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Property;

/**
 * In Accuracy DQ, the mismatched data is actually stored to file. <p/>
 * 
 * Each of this has a corresponding DqMetricsValue, by {modelName, timestamp}
 */
// uniq{model, timestamp}
@Entity("dq_missed_file_path_lkp")
public class SampleFilePathLKP extends IdEntity {

    @Property("modelName")
    private String modelName;

    @Property("hdfsPath")
    private String hdfsPath;

    @Property("timestamp")
    private long timestamp;

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

}
