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
import java.util.Collections;
import java.util.List;

public class AssetLevelMetrics {
	private String name;
	private float dq;
	private int dqfail;
	private long timestamp;
	private String metricType;
	private String assetName;

	private List<AssetLevelMetricsDetail> details = new ArrayList<AssetLevelMetricsDetail>();

	public AssetLevelMetrics() { }

	public AssetLevelMetrics(String name) {
		this.name = name;
	}

	public AssetLevelMetrics(String name, float dq, long timestamp) {
		this.name = name;
		this.dq = dq;
		this.timestamp = timestamp;
	}

	public AssetLevelMetrics(String name, String metricType, float dq, long timestamp, int dqfail) {
		this.name = name;
		this.dq = dq;
		this.timestamp = timestamp;
		this.dqfail = dqfail;
		this.metricType = metricType;
	}

	public AssetLevelMetrics(AssetLevelMetrics other, int count) {
		this.name = other.getName();
		this.dq = other.getDq();
		this.timestamp = other.getTimestamp();
		this.dqfail = other.getDqfail();
		this.metricType = other.getMetricType();
		List<AssetLevelMetricsDetail> otherDetail = other.getDetails();
		if(count == -1) count = other.getDetails().size();
		if(other.getDetails().size()<count) count = other.getDetails().size();
		Collections.sort(otherDetail);
		for(int i=0;i<count;i++) {
			AssetLevelMetricsDetail tempAssetLevelMetricsDetail = otherDetail.get(i);
			details.add(new AssetLevelMetricsDetail(tempAssetLevelMetricsDetail));
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public float getDq() {
		return dq;
	}

	public void setDq(float dq) {
		this.dq = dq;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public List<AssetLevelMetricsDetail> getDetails() {
		return details;
	}

	public void setDetails(List<AssetLevelMetricsDetail> details) {
		this.details = details;
	}

	public void addAssetLevelMetricsDetail(AssetLevelMetricsDetail dq)
	{
		this.details.add(dq);
	}

	public int getDqfail() {
		return dqfail;
	}

	public void setDqfail(int dqfail) {
		this.dqfail = dqfail;
	}

	public String getMetricType() {
		return metricType;
	}

	public void setMetricType(String metricType) {
		this.metricType = metricType;
	}

	public String getAssetName() {
		return assetName;
	}

	public void setAssetName(String asseetName) {
		this.assetName = asseetName;
	}









}
