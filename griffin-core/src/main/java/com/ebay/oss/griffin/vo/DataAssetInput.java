/*
 * Copyright (c) 2016 eBay Software Foundation. Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.ebay.oss.griffin.vo;

import java.util.List;

import com.ebay.oss.griffin.domain.DataSchema;
import com.ebay.oss.griffin.domain.PartitionFormat;

public class DataAssetInput extends BaseObj {

    private String assetName;

    private String assetType;

    private String assetHDFSPath;

    private String system;

    private String platform;

    private String owner;

    private String partition;

    private List<DataSchema> schema;

    private List<PartitionFormat> partitions;

    public String getAssetName() {
        return assetName;
    }

    public void setAssetName(String assetName) {
        this.assetName = assetName;
    }

    public String getAssetType() {
        return assetType;
    }

    public void setAssetType(String assetType) {
        this.assetType = assetType;
    }

    public String getAssetHDFSPath() {
        return assetHDFSPath;
    }

    public void setAssetHDFSPath(String assetHDFSPath) {
        this.assetHDFSPath = assetHDFSPath;
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public List<DataSchema> getSchema() {
        return schema;
    }

    public void setSchema(List<DataSchema> schema) {
        this.schema = schema;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public List<PartitionFormat> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionFormat> partitions) {
        this.partitions = partitions;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }


}
