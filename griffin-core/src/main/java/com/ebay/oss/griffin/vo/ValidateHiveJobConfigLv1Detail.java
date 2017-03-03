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

public class ValidateHiveJobConfigLv1Detail {
	public int colId;
	public String colName;
	public List<ValidateHiveJobConfigLv2Detail> metrics = new ArrayList<ValidateHiveJobConfigLv2Detail>();

	public ValidateHiveJobConfigLv1Detail() { }

	public ValidateHiveJobConfigLv1Detail(int colId, String colName, List<ValidateHiveJobConfigLv2Detail> metrics) {
		this.colId = colId;
		this.colName = colName;
		this.metrics = metrics;
	}

	public int getColId() {
		return colId;
	}

	public void setColId(int colId) {
		this.colId = colId;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public List<ValidateHiveJobConfigLv2Detail> getMetrics() {
		return metrics;
	}

	public void setMetrics(List<ValidateHiveJobConfigLv2Detail> metrics) {
		this.metrics = metrics;
	}



}
