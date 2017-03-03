/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.ebay.oss.griffin.vo;

import java.util.ArrayList;
import java.util.List;

public class AccuracyHiveJobConfig {
	public String source;
	public String target;
	public List<AccuracyHiveJobConfigDetail> accuracyMapping = new ArrayList<AccuracyHiveJobConfigDetail>();
	public List<PartitionConfig> srcPartitions = new ArrayList<PartitionConfig>();
	public List<List<PartitionConfig>> tgtPartitions = new ArrayList<List<PartitionConfig>>();

	public AccuracyHiveJobConfig() { }

	public AccuracyHiveJobConfig(String source, String target) {
		this.source = source;
		this.target = target;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public List<AccuracyHiveJobConfigDetail> getAccuracyMapping() {
		return accuracyMapping;
	}

	public void setAccuracyMapping(List<AccuracyHiveJobConfigDetail> accuracyMapping) {
		this.accuracyMapping = accuracyMapping;
	}

	public List<PartitionConfig> getSrcPartitions() {
		return srcPartitions;
	}

	public void setSrcPartitions(List<PartitionConfig> srcPartitions) {
		if(srcPartitions!=null) this.srcPartitions = srcPartitions;
	}

	public List<List<PartitionConfig>> getTgtPartitions() {
		return tgtPartitions;
	}

	public void setTgtPartitions(List<List<PartitionConfig>> tgtPartitions) {
		if(tgtPartitions!=null) this.tgtPartitions = tgtPartitions;
	}






}
