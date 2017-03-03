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


public class OverViewStatistics {

	private int assets;
	private int metrics;
	private DQHealthStats status;

	public int getAssets() {
		return assets;
	}
	public void setAssets(int assets) {
		this.assets = assets;
	}
	public int getMetrics() {
		return metrics;
	}
	public void setMetrics(int metrics) {
		this.metrics = metrics;
	}
	public DQHealthStats getStatus() {
		return status;
	}
	public void setStatus(DQHealthStats status) {
		this.status = status;
	}


}
